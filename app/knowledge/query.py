import json
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from qdrant_client import models as qdrant_models
from sqlalchemy.orm import Session

from app.artifacts import create_artifact_record
from app.knowledge.domains import DOMAIN_PRECEDENCE, KnowledgeDomain
from app.knowledge.embeddings import get_sentence_transformers_provider
from app.knowledge.qdrant import close_qdrant_client, collection_name_for_domain, get_qdrant_client
from app.knowledge.router import route_query
from app.logging_utils import log_event
from app.limits import EMBEDDING_MODEL_DEFAULT, KNOWLEDGE_QUERY_MAX_DOMAINS, TOP_K_PER_DOMAIN
from app.models import Artifact, KnowledgeChunk, KnowledgeDocument
from app.storage import get_client, get_object

DOMAIN_MULTIPLIERS = {
    KnowledgeDomain.BELIEF.value: 1.15,
    KnowledgeDomain.PROJECT.value: 1.10,
    KnowledgeDomain.OPS.value: 1.08,
    KnowledgeDomain.INFLUENCE.value: 1.03,
    KnowledgeDomain.COGNITION.value: 1.00,
    KnowledgeDomain.REFERENCE.value: 0.97,
    KnowledgeDomain.ARCHIVE.value: 0.92,
}
PREFER_BOOST = 0.03
CONFIDENCE_ORDER = {"low": 0, "medium": 1, "high": 2}
QUERY_DOMAIN_MODES = {"only", "prefer", "exclude"}


def _read_artifact_json(artifact: Artifact) -> Dict[str, Any]:
    obj = get_object(get_client(), artifact.path)
    try:
        return json.loads(obj.read().decode("utf-8"))
    finally:
        obj.close()
        obj.release_conn()


def normalize_query_filters(filters: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    filters = filters or {}
    normalized: Dict[str, Any] = {}

    sources = filters.get("source")
    if sources is not None:
        if not isinstance(sources, list):
            raise ValueError("filters.source must be a list")
        normalized_sources = []
        for source in sources:
            if not isinstance(source, str) or not source.strip():
                raise ValueError("filters.source must contain non-empty strings")
            normalized_sources.append(source.strip().lower())
        normalized["source"] = sorted(set(normalized_sources))

    confidence_min = filters.get("confidence_min")
    if confidence_min is not None:
        if not isinstance(confidence_min, str):
            raise ValueError("filters.confidence_min must be a string")
        confidence_value = confidence_min.strip().lower()
        if confidence_value not in CONFIDENCE_ORDER:
            raise ValueError("filters.confidence_min must be one of low, medium, high")
        normalized["confidence_min"] = confidence_value

    return normalized


def normalize_domains(domains: Optional[List[str]]) -> List[str]:
    if not domains:
        return []
    ordered: List[str] = []
    seen = set()
    for domain in DOMAIN_PRECEDENCE:
        if any(str(item) == domain.value for item in domains):
            ordered.append(domain.value)
            seen.add(domain.value)
    for domain in domains:
        value = KnowledgeDomain(domain).value
        if value not in seen:
            ordered.append(value)
            seen.add(value)
    return ordered


def _all_domains() -> List[str]:
    return [domain.value for domain in DOMAIN_PRECEDENCE]


def determine_search_domains(
    query: str,
    requested_domains: Optional[List[str]],
    domain_mode: Optional[str],
    max_domains: int,
) -> Tuple[List[str], str, Dict[str, Any], List[str], bool]:
    notes: List[str] = []
    used_router = not requested_domains

    if used_router:
        routing = route_query(query)
        searched_domains = normalize_domains(routing["searched_domains"])
        effective_mode = "only"
        routing_meta = {
            "used_router": True,
            "routing_reason": routing["routing_reason"],
            "routing_mode": routing["routing_mode"],
        }
        domains_requested = []
    else:
        normalized_requested = normalize_domains(requested_domains)
        effective_mode = domain_mode or "prefer"
        if effective_mode not in QUERY_DOMAIN_MODES:
            raise ValueError("domain_mode must be one of only, prefer, exclude")
        all_domains = _all_domains()
        if effective_mode == "only":
            searched_domains = normalized_requested
        elif effective_mode == "prefer":
            searched_domains = all_domains
        else:
            searched_domains = [domain for domain in all_domains if domain not in set(normalized_requested)]
        routing_meta = {
            "used_router": False,
            "routing_reason": None,
            "routing_mode": None,
        }
        domains_requested = normalized_requested

    if len(searched_domains) > max_domains:
        truncated = searched_domains[:max_domains]
        notes.append(
            f"searched domains truncated from {len(searched_domains)} to {len(truncated)} by KNOWLEDGE_QUERY_MAX_DOMAINS"
        )
        searched_domains = truncated

    return searched_domains, effective_mode, routing_meta, domains_requested, used_router


def _build_qdrant_filter(filters: Dict[str, Any]) -> Optional[qdrant_models.Filter]:
    source_values = filters.get("source")
    if not source_values:
        return None
    return qdrant_models.Filter(
        must=[
            qdrant_models.FieldCondition(
                key="source",
                match=qdrant_models.MatchAny(any=source_values),
            )
        ]
    )


def _passes_python_filters(payload: Dict[str, Any], filters: Dict[str, Any]) -> bool:
    allowed_sources = filters.get("source")
    if allowed_sources is not None and payload.get("source") not in allowed_sources:
        return False

    confidence_min = filters.get("confidence_min")
    if confidence_min is not None:
        current = str(payload.get("confidence") or "low").lower()
        if CONFIDENCE_ORDER.get(current, 0) < CONFIDENCE_ORDER[confidence_min]:
            return False
    return True


def _parse_chunk_index(point_id: Any) -> Optional[int]:
    try:
        raw = str(point_id)
        return int(raw.rsplit(":", 1)[1])
    except Exception:
        return None


def _load_chunk_snippet(
    db: Session,
    doc_id: str,
    qdrant_point_id: str,
    chunk_index: Optional[int],
) -> Tuple[Optional[str], Optional[str]]:
    chunk_row = (
        db.query(KnowledgeChunk)
        .filter(KnowledgeChunk.document_id == doc_id, KnowledgeChunk.qdrant_point_id == qdrant_point_id)
        .first()
    )
    if not chunk_row and chunk_index is not None:
        chunk_row = (
            db.query(KnowledgeChunk)
            .filter(KnowledgeChunk.document_id == doc_id, KnowledgeChunk.chunk_index == chunk_index)
            .first()
        )
    if not chunk_row:
        return None, "knowledge chunk row missing"

    artifact = db.query(Artifact).filter(Artifact.id == chunk_row.text_artifact_id).first()
    if not artifact:
        return None, "knowledge_document_chunks artifact missing"

    try:
        payload = _read_artifact_json(artifact)
        chunks = payload.get("chunks")
        if not isinstance(chunks, list):
            return None, "knowledge_document_chunks payload malformed"
        for chunk in chunks:
            if not isinstance(chunk, dict):
                continue
            if int(chunk.get("chunk_index", -1)) == int(chunk_row.chunk_index):
                text = chunk.get("text")
                if not isinstance(text, str):
                    return None, "chunk text missing"
                snippet = text.strip()
                if len(snippet) > 280:
                    snippet = snippet[:280].rstrip() + "..."
                return snippet, None
        return None, "chunk_index not found in chunks artifact"
    except Exception as exc:
        return None, str(exc)


def _artifact_for_results(db: Session, task_id: str, payload: Dict[str, Any], logger) -> Artifact:
    return create_artifact_record(
        db=db,
        task_id=task_id,
        artifact_type="knowledge_query_results",
        content_type="application/json",
        data=json.dumps(payload).encode("utf-8"),
        metadata={"query": payload["query"], "result_count": len(payload["results"])},
        event_logger=logger,
    )


def _artifact_for_audit(db: Session, task_id: str, payload: Dict[str, Any], logger) -> Artifact:
    return create_artifact_record(
        db=db,
        task_id=task_id,
        artifact_type="knowledge_query_audit",
        content_type="application/json",
        data=json.dumps(payload).encode("utf-8"),
        metadata={"query": payload["query"], "domains_searched": payload["domains_searched"]},
        event_logger=logger,
    )


def run_knowledge_query_task(db: Session, task_id: str, params: Dict[str, Any], logger) -> Dict[str, Any]:
    query = str(params.get("query") or "").strip()
    qdrant_client = None
    try:
        if not query:
            raise ValueError("query is required")

        filters = normalize_query_filters(params.get("filters"))
        top_k_per_domain = int(params.get("top_k_per_domain", TOP_K_PER_DOMAIN))
        if top_k_per_domain <= 0:
            raise ValueError("top_k_per_domain must be positive")

        searched_domains, effective_mode, routing_meta, domains_requested, _ = determine_search_domains(
            query=query,
            requested_domains=params.get("domains"),
            domain_mode=params.get("domain_mode"),
            max_domains=KNOWLEDGE_QUERY_MAX_DOMAINS,
        )
        if not searched_domains:
            raise ValueError("no domains selected for search")

        provider = get_sentence_transformers_provider(EMBEDDING_MODEL_DEFAULT)
        embedding_revision = provider.embedding_revision
        query_vector = provider.embed([query])[0]
        qdrant_client = get_qdrant_client()
        qdrant_filter = _build_qdrant_filter(filters)

        explicit_domains = set(domains_requested)
        per_domain: Dict[str, Dict[str, Any]] = {}
        merged_hits: List[Dict[str, Any]] = []
        notes: List[str] = []
        snippet_failures = 0

        for domain in _all_domains():
            collection = collection_name_for_domain(domain)
            per_domain[domain] = {
                "collection": collection,
                "searched": domain in searched_domains,
                "exists": False,
                "raw_hits": 0,
                "post_filter_hits": 0,
                "top_raw_scores": [],
            }
            if domain not in searched_domains:
                continue

            exists = qdrant_client.collection_exists(collection)
            per_domain[domain]["exists"] = exists
            if not exists:
                notes.append(f"{domain} collection missing; skipped")
                continue

            try:
                raw_hits = qdrant_client.search(
                    collection_name=collection,
                    query_vector=query_vector,
                    limit=top_k_per_domain,
                    with_payload=True,
                    with_vectors=False,
                    query_filter=qdrant_filter,
                )
            except Exception as exc:
                notes.append(f"{domain} search failed: {exc}")
                continue

            per_domain[domain]["raw_hits"] = len(raw_hits)
            per_domain[domain]["top_raw_scores"] = [round(float(hit.score), 6) for hit in raw_hits[:2]]

            domain_hits: List[Dict[str, Any]] = []
            for hit in raw_hits:
                payload = dict(hit.payload or {})
                if not _passes_python_filters(payload, filters):
                    continue
                raw_score = float(hit.score)
                weighted_score = raw_score * DOMAIN_MULTIPLIERS[domain]
                if effective_mode == "prefer" and explicit_domains and domain in explicit_domains:
                    weighted_score += PREFER_BOOST
                qdrant_point_id = str(payload.get("qdrant_point_id") or hit.id)
                chunk_index_value = payload.get("chunk_index")
                try:
                    chunk_index = int(chunk_index_value) if chunk_index_value is not None else None
                except Exception:
                    chunk_index = None
                if chunk_index is None:
                    chunk_index = _parse_chunk_index(hit.id)
                snippet, snippet_error = _load_chunk_snippet(
                    db=db,
                    doc_id=str(payload.get("doc_id")),
                    qdrant_point_id=qdrant_point_id,
                    chunk_index=chunk_index,
                )
                if snippet_error:
                    snippet_failures += 1
                    notes.append(f"{domain} snippet resolution issue for {qdrant_point_id}: {snippet_error}")

                domain_hits.append(
                    {
                        "domain": domain,
                        "raw_score": raw_score,
                        "score": weighted_score,
                        "doc_id": str(payload.get("doc_id")),
                        "artifact_id": str(payload.get("artifact_id")),
                        "chunk_index": chunk_index,
                        "snippet": snippet,
                        "qdrant_point_id": qdrant_point_id,
                        "promotion_key": payload.get("promotion_key"),
                        "embedding_model": payload.get("embedding_model"),
                        "embedding_revision": payload.get("embedding_revision"),
                        "chunker_version": payload.get("chunker_version"),
                        "chunk_params": payload.get("chunk_params"),
                    }
                )
            per_domain[domain]["post_filter_hits"] = len(domain_hits)
            merged_hits.extend(domain_hits)

        seen = set()
        deduped_hits: List[Dict[str, Any]] = []
        for hit in merged_hits:
            key = (hit["doc_id"], hit["chunk_index"], hit["qdrant_point_id"])
            if key in seen:
                continue
            seen.add(key)
            deduped_hits.append(hit)

        result_limit = min(50, top_k_per_domain * len(searched_domains))
        ranked_hits = sorted(deduped_hits, key=lambda item: item["score"], reverse=True)[:result_limit]

        results_payload = {
            "query": query,
            "results": [
                {
                    "rank": index + 1,
                    "domain": hit["domain"],
                    "score": round(float(hit["score"]), 6),
                    "doc_id": hit["doc_id"],
                    "artifact_id": hit["artifact_id"],
                    "chunk_index": hit["chunk_index"],
                    "snippet": hit["snippet"],
                    "provenance": {
                        "promotion_key": hit["promotion_key"],
                        "embedding_model": hit["embedding_model"],
                        "embedding_revision": hit["embedding_revision"],
                        "chunker_version": hit["chunker_version"],
                        "chunk_params": hit["chunk_params"],
                    },
                }
                for index, hit in enumerate(ranked_hits)
            ],
        }
        results_artifact = _artifact_for_results(db, task_id, results_payload, logger)

        if snippet_failures:
            notes.append(f"{snippet_failures} snippets could not be resolved")

        audit_payload = {
            "query": query,
            "domains_requested": domains_requested,
            "domain_mode": effective_mode,
            "domains_searched": searched_domains,
            "routing": routing_meta,
            "top_k_per_domain": top_k_per_domain,
            "filters_applied": {
                "source": filters.get("source"),
                "confidence_min": filters.get("confidence_min"),
            },
            "embedding_model": EMBEDDING_MODEL_DEFAULT,
            "embedding_revision": embedding_revision,
            "per_domain": per_domain,
            "scoring": {
                "precedence_multipliers": DOMAIN_MULTIPLIERS,
                "prefer_boost": PREFER_BOOST,
            },
            "notes": notes,
        }
        audit_artifact = _artifact_for_audit(db, task_id, audit_payload, logger)

        summary = (
            f"Returned {len(ranked_hits)} results from {'+'.join(searched_domains)} "
            f"(top_k_per_domain={top_k_per_domain})"
        )
        if ranked_hits:
            log_event(
                logger,
                "knowledge_query_completed",
                task_id=task_id,
                query=query,
                domains_requested=domains_requested,
                domains_searched=searched_domains,
                top_k_per_domain=top_k_per_domain,
                result_count=len(ranked_hits),
            )
        else:
            log_event(
                logger,
                "knowledge_query_empty",
                task_id=task_id,
                query=query,
                domains_requested=domains_requested,
                domains_searched=searched_domains,
                top_k_per_domain=top_k_per_domain,
                result_count=0,
            )
        return {
            "artifact_ids": [results_artifact.id, audit_artifact.id],
            "summary": summary,
        }
    except Exception as exc:
        log_event(
            logger,
            "knowledge_query_failed",
            task_id=task_id,
            query=query,
            domains_requested=params.get("domains") or [],
            domains_searched=[],
            top_k_per_domain=params.get("top_k_per_domain", TOP_K_PER_DOMAIN),
            result_count=0,
            error=str(exc),
        )
        raise
    finally:
        if qdrant_client is not None:
            close_qdrant_client(qdrant_client)
