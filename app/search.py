import hashlib
import json
import os
import urllib.parse
import urllib.request
import urllib.error
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Optional, Tuple

import redis
from sqlalchemy.orm import Session

from app.artifacts import create_artifact_record
from app.cache import SEARCH_CACHE_TTL_SECONDS, build_cache_key, get_cache, set_cache
from app.limits import MAX_BATCH_SIZE
from app.logging_utils import log_event
from app.models import Artifact
from app.storage import get_client, get_object

BRAVE_API_URL = os.getenv("BRAVE_API_URL", "https://api.search.brave.com/res/v1/web/search")
BRAVE_API_KEY = os.getenv("BRAVE_API_KEY")
SEARCH_MAX_RESULTS = int(os.getenv("SEARCH_MAX_RESULTS", "10"))
EXA_API_URL = os.getenv("EXA_API_URL", "https://api.exa.ai/search")
EXA_API_KEY = os.getenv("EXA_API_KEY")


class SearchProvider:
    name = ""

    def search(self, query: str, recency_days: int) -> Dict[str, Any]:
        raise NotImplementedError


class BraveSearchProvider(SearchProvider):
    name = "brave"

    def search(self, query: str, recency_days: int) -> Dict[str, Any]:
        if not BRAVE_API_KEY:
            raise RuntimeError("BRAVE_API_KEY is not set")

        params = {
            "q": query,
            "count": str(SEARCH_MAX_RESULTS),
        }
        freshness = _recency_to_freshness(recency_days)
        if freshness:
            params["freshness"] = freshness

        url = f"{BRAVE_API_URL}?{urllib.parse.urlencode(params)}"
        req = urllib.request.Request(
            url,
            headers={
                "Accept": "application/json",
                "X-Subscription-Token": BRAVE_API_KEY,
            },
        )
        with urllib.request.urlopen(req, timeout=15) as resp:
            body = resp.read().decode("utf-8")
        return json.loads(body)


class ExaSearchProvider(SearchProvider):
    name = "exa"

    def search(self, query: str, recency_days: int) -> Dict[str, Any]:
        if not EXA_API_KEY:
            raise RuntimeError("EXA_API_KEY is not set")

        # Exa Search API: POST https://api.exa.ai/search
        # Auth: x-api-key header OR Authorization: Bearer <key>
        payload: Dict[str, Any] = {
            "query": query,
            "numResults": SEARCH_MAX_RESULTS,
            "contents": {"text": True},
        }
        if recency_days and recency_days > 0:
            start_date = datetime.utcnow() - timedelta(days=recency_days)
            payload["startPublishedDate"] = start_date.isoformat() + "Z"

        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            EXA_API_URL,
            data=data,
            headers={
                "Accept": "application/json",
                "Content-Type": "application/json",
                "x-api-key": EXA_API_KEY,
                "Authorization": f"Bearer {EXA_API_KEY}",
                "User-Agent": "Narsil-Exa-Client/1.0",
            },
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=15) as resp:
                body = resp.read().decode("utf-8")
            return json.loads(body)
        except urllib.error.HTTPError as exc:
            try:
                error_body = exc.read().decode("utf-8")
            except Exception:
                error_body = ""
            error_body = error_body[:500]
            raise RuntimeError(f"exa_http_{exc.code}:{error_body}") from exc


class TaskCancelled(Exception):
    pass


class TaskRuntimeExceeded(Exception):
    pass


def _normalize_query(query: str) -> str:
    if not isinstance(query, str) or not query.strip():
        raise ValueError("query is required")
    return " ".join(query.split()).strip()


def normalize_params(params: Dict[str, Any]) -> Dict[str, Any]:
    query = _normalize_query(params.get("query"))

    sources = params.get("sources")
    if sources is None:
        sources = ["brave"]
    if not isinstance(sources, list) or not sources:
        raise ValueError("sources must be a non-empty list")
    normalized_sources = []
    for source in sources:
        if not isinstance(source, str) or not source.strip():
            raise ValueError("sources must contain non-empty strings")
        normalized_sources.append(source.strip().lower())

    recency_days = params.get("recency_days", 7)
    if recency_days is None:
        recency_days = 7
    if not isinstance(recency_days, int) or recency_days < 0:
        raise ValueError("recency_days must be a non-negative integer")

    return {
        "query": query,
        "sources": sorted(set(normalized_sources)),
        "recency_days": recency_days,
    }


def normalize_batch_params(params: Dict[str, Any]) -> Dict[str, Any]:
    queries = params.get("queries")
    if not isinstance(queries, list) or not queries:
        raise ValueError("queries must be a non-empty list")
    normalized_queries: List[str] = []
    for query in queries:
        normalized_queries.append(_normalize_query(query))
    if len(normalized_queries) > MAX_BATCH_SIZE:
        raise ValueError("batch_size_exceeded")

    sources = params.get("sources")
    if sources is None:
        sources = ["brave"]
    if not isinstance(sources, list) or not sources:
        raise ValueError("sources must be a non-empty list")
    normalized_sources = []
    for source in sources:
        if not isinstance(source, str) or not source.strip():
            raise ValueError("sources must contain non-empty strings")
        normalized_sources.append(source.strip().lower())

    recency_days = params.get("recency_days", 7)
    if recency_days is None:
        recency_days = 7
    if not isinstance(recency_days, int) or recency_days < 0:
        raise ValueError("recency_days must be a non-negative integer")

    return {
        "queries": sorted(normalized_queries),
        "sources": sorted(set(normalized_sources)),
        "recency_days": recency_days,
    }


def compute_cache_key(query: str, sources: List[str], recency_days: int) -> str:
    cache_input = f"{query}|{','.join(sorted(sources))}|{recency_days}"
    return hashlib.sha256(cache_input.encode("utf-8")).hexdigest()


def run_search_task(
    db: Session,
    task_id: str,
    params: Dict[str, Any],
    redis_client: redis.Redis,
    logger,
) -> Dict[str, Any]:
    normalized = normalize_params(params)
    query = normalized["query"]
    sources = normalized["sources"]
    recency_days = normalized["recency_days"]

    log_event(
        logger,
        "search_task_started",
        task_id=task_id,
        query=query,
        sources=sources,
        recency_days=recency_days,
    )

    cache_key = build_cache_key(compute_cache_key(query, sources, recency_days))
    cached_value = get_cache(redis_client, cache_key)
    if cached_value and "artifact_ids" in cached_value and "summary" in cached_value:
        log_event(logger, "search_cache_hit", task_id=task_id, cache_key=cache_key)
        log_event(logger, "search_cached_hit", task_id=task_id, provider=",".join(sources))
        log_event(logger, "search_task_completed", task_id=task_id, cache_key=cache_key, cached=True)
        return cached_value

    log_event(logger, "search_cache_miss", task_id=task_id, cache_key=cache_key)

    try:
        results, raw_payloads, sources_used = call_providers(query, sources, recency_days, logger)
        artifact_ids = []
        for provider_name, raw_payload in raw_payloads:
            data = json.dumps(raw_payload).encode("utf-8")
            artifact = create_artifact_record(
                db=db,
                task_id=task_id,
                artifact_type="search_raw",
                content_type="application/json",
                data=data,
                event_logger=logger,
            )
            artifact_ids.append(artifact.id)

        summary = f"{len(results)} results from {', '.join(sources_used)}"
        normalized_payload = {
            "query": query,
            "results": results,
            "sources_used": sources_used,
            "retrieved_at": datetime.utcnow().isoformat() + "Z",
        }
        normalized_artifact = create_artifact_record(
            db=db,
            task_id=task_id,
            artifact_type="search_results",
            content_type="application/json",
            data=json.dumps(normalized_payload).encode("utf-8"),
            event_logger=logger,
        )

        result_payload = {
            "artifact_ids": [normalized_artifact.id] + artifact_ids,
            "summary": summary,
        }
        try:
            set_cache(redis_client, cache_key, result_payload, SEARCH_CACHE_TTL_SECONDS)
        except Exception:
            pass

        log_event(logger, "search_task_completed", task_id=task_id, cache_key=cache_key, cached=False)
        return result_payload
    except Exception as exc:
        log_event(logger, "search_task_failed", task_id=task_id, error=str(exc))
        raise


def _load_cached_results(
    db: Session,
    cached_value: Dict[str, Any],
    logger,
) -> Optional[Dict[str, Any]]:
    if not isinstance(cached_value, dict):
        return None
    artifact_ids = cached_value.get("artifact_ids")
    if not isinstance(artifact_ids, list) or not artifact_ids:
        return None
    artifact = (
        db.query(Artifact)
        .filter(Artifact.id.in_(artifact_ids), Artifact.type == "search_results")
        .first()
    )
    if not artifact:
        return None
    obj = None
    try:
        client = get_client()
        obj = get_object(client, artifact.path)
        body = obj.read()
        payload = json.loads(body.decode("utf-8"))
        return payload if isinstance(payload, dict) else None
    except Exception:
        return None
    finally:
        try:
            if obj is not None:
                obj.close()
        except Exception:
            pass


def run_search_batch_task(
    db: Session,
    task_id: str,
    params: Dict[str, Any],
    redis_client: redis.Redis,
    logger,
    normalized: Optional[Dict[str, Any]] = None,
    should_abort: Optional[Callable[[], Optional[str]]] = None,
) -> Dict[str, Any]:
    if normalized is None:
        normalized = normalize_batch_params(params)
    queries = normalized["queries"]
    sources = normalized["sources"]
    recency_days = normalized["recency_days"]

    log_event(
        logger,
        "search_batch_started",
        task_id=task_id,
        query_count=len(queries),
        sources=sources,
        recency_days=recency_days,
    )

    grouped_results: List[Dict[str, Any]] = []
    artifact_ids: List[str] = []

    for query in queries:
        if should_abort:
            reason = should_abort()
            if reason == "cancelled":
                raise TaskCancelled()
            if reason == "runtime_limit":
                raise TaskRuntimeExceeded()

        cache_key = build_cache_key(compute_cache_key(query, sources, recency_days))
        cached_value = get_cache(redis_client, cache_key)
        cached_payload = _load_cached_results(db, cached_value, logger) if cached_value else None
        if cached_payload:
            log_event(logger, "search_batch_cached_hit", task_id=task_id, cache_key=cache_key, query=query)
            log_event(logger, "search_cached_hit", task_id=task_id, provider=",".join(sources), query=query)
            grouped_results.append(
                {
                    "query": cached_payload.get("query", query),
                    "results": cached_payload.get("results", []),
                    "sources_used": cached_payload.get("sources_used", sources),
                    "retrieved_at": cached_payload.get("retrieved_at"),
                    "cached": True,
                }
            )
            cached_artifact_ids = cached_value.get("artifact_ids") if isinstance(cached_value, dict) else None
            if isinstance(cached_artifact_ids, list):
                artifact_ids.extend(cached_artifact_ids)
            continue

        log_event(logger, "search_batch_provider_call", task_id=task_id, query=query, sources=sources)
        results, raw_payloads, sources_used = call_providers(query, sources, recency_days, logger)
        per_query_artifacts: List[str] = []
        for provider_name, raw_payload in raw_payloads:
            data = json.dumps(raw_payload).encode("utf-8")
            artifact = create_artifact_record(
                db=db,
                task_id=task_id,
                artifact_type="search_raw",
                content_type="application/json",
                data=data,
                event_logger=logger,
            )
            per_query_artifacts.append(artifact.id)

        summary = f"{len(results)} results from {', '.join(sources_used)}"
        normalized_payload = {
            "query": query,
            "results": results,
            "sources_used": sources_used,
            "retrieved_at": datetime.utcnow().isoformat() + "Z",
        }
        normalized_artifact = create_artifact_record(
            db=db,
            task_id=task_id,
            artifact_type="search_results",
            content_type="application/json",
            data=json.dumps(normalized_payload).encode("utf-8"),
            event_logger=logger,
        )
        per_query_artifacts.insert(0, normalized_artifact.id)
        artifact_ids.extend(per_query_artifacts)
        grouped_results.append(
            {
                "query": query,
                "results": results,
                "sources_used": sources_used,
                "retrieved_at": normalized_payload["retrieved_at"],
                "cached": False,
            }
        )

        cache_payload = {
            "artifact_ids": per_query_artifacts,
            "summary": summary,
        }
        try:
            set_cache(redis_client, cache_key, cache_payload, SEARCH_CACHE_TTL_SECONDS)
        except Exception:
            pass

    batch_payload = {"queries": grouped_results}
    batch_artifact = create_artifact_record(
        db=db,
        task_id=task_id,
        artifact_type="search_batch_results",
        content_type="application/json",
        data=json.dumps(batch_payload).encode("utf-8"),
        event_logger=logger,
    )

    unique_ids: List[str] = []
    seen = set()
    for artifact_id in artifact_ids:
        if artifact_id in seen:
            continue
        seen.add(artifact_id)
        unique_ids.append(artifact_id)

    result_payload = {
        "artifact_ids": [batch_artifact.id] + unique_ids,
        "summary": f"Batch search completed: {len(queries)} queries",
    }

    log_event(logger, "search_batch_completed", task_id=task_id, query_count=len(queries), artifact_id=batch_artifact.id)
    return result_payload


def call_providers(
    query: str,
    sources: List[str],
    recency_days: int,
    logger,
) -> Tuple[List[Dict[str, Any]], List[Tuple[str, Dict[str, Any]]], List[str]]:
    results: List[Dict[str, Any]] = []
    raw_payloads: List[Tuple[str, Dict[str, Any]]] = []
    sources_used: List[str] = []

    for source in sources:
        if source == "brave":
            provider = BraveSearchProvider()
        elif source == "exa":
            provider = ExaSearchProvider()
        else:
            raise RuntimeError(f"unsupported search provider: {source}")

        log_event(logger, "search_provider_called", provider=provider.name, query=query)
        try:
            raw = provider.search(query, recency_days)
        except Exception as exc:
            log_event(logger, "search_provider_error", provider=provider.name, query=query, error=str(exc))
            raise
        raw_payloads.append((provider.name, raw))
        if provider.name == "exa":
            normalized = normalize_exa_results(raw)
        else:
            normalized = normalize_brave_results(raw)
        results.extend(normalized)
        sources_used.append(provider.name)

    return results, raw_payloads, sources_used


def normalize_brave_results(raw: Dict[str, Any]) -> List[Dict[str, Any]]:
    items = raw.get("web", {}).get("results", [])
    normalized: List[Dict[str, Any]] = []
    for item in items:
        title = item.get("title")
        url = item.get("url") or item.get("link")
        snippet = item.get("description") or item.get("snippet")
        published = (
            item.get("published_at")
            or item.get("published_time")
            or item.get("published_date")
            or item.get("date")
        )
        if not title or not url:
            continue
        normalized.append(
            {
                "title": title,
                "url": url,
                "snippet": snippet or "",
                "published_at": published,
                "source": "brave",
            }
        )
    return normalized


def normalize_exa_results(raw: Dict[str, Any]) -> List[Dict[str, Any]]:
    items = raw.get("results", [])
    normalized: List[Dict[str, Any]] = []
    for item in items:
        title = item.get("title")
        url = item.get("url")
        snippet = item.get("summary") or item.get("text") or ""
        published = item.get("publishedDate") or item.get("published_at") or item.get("published_date")
        if not title or not url:
            continue
        normalized.append(
            {
                "title": title,
                "url": url,
                "snippet": snippet[:1000],
                "published_at": published,
                "source": "exa",
            }
        )
    return normalized


def _recency_to_freshness(recency_days: int) -> str:
    if recency_days <= 0:
        return ""
    if recency_days <= 1:
        return "pd"
    if recency_days <= 7:
        return "pw"
    if recency_days <= 31:
        return "pm"
    return "py"
