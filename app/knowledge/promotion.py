import hashlib
import json
import math
import signal
import time
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from qdrant_client import models as qdrant_models
from sqlalchemy.orm import Session

from app.artifacts import create_artifact_record
from app.knowledge.chunking import CHUNKER_VERSION, chunk_text, normalize_chunk_params
from app.knowledge.domains import KnowledgeDomain
from app.knowledge.embeddings import EmbeddingProviderInitError, get_sentence_transformers_provider
from app.knowledge.qdrant import collection_name_for_domain, delete_points, ensure_collection, upsert_points
from app.logging_utils import log_event
from app.limits import (
    EMBEDDING_MODEL_DEFAULT,
    KNOWLEDGE_PROMOTION_EMBED_BATCH_SIZE,
    KNOWLEDGE_PROMOTION_MAX_CHUNKS,
    KNOWLEDGE_PROMOTION_MAX_INPUT_BYTES,
    KNOWLEDGE_PROMOTION_MODEL_INIT_TIMEOUT_SECONDS,
    KNOWLEDGE_PROMOTION_UPSERT_BATCH_SIZE,
    TASK_MAX_RUNTIME_SECONDS,
)
from app.models import Artifact, KnowledgeChunk, KnowledgeDocument, Task
from app.search import TaskCancelled, TaskRuntimeExceeded
from app.storage import get_client, get_object


def canonical_json(value: Dict[str, Any]) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def compute_promotion_key(
    artifact_id: str,
    domain: str,
    embedding_model: str,
    embedding_revision: str,
    chunker_version: str,
    chunk_params: Dict[str, Any],
) -> str:
    payload = "|".join(
        [
            artifact_id,
            domain,
            embedding_model,
            embedding_revision,
            chunker_version,
            canonical_json(chunk_params),
        ]
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _read_artifact_bytes(artifact: Artifact) -> bytes:
    obj = get_object(get_client(), artifact.path)
    try:
        return obj.read()
    finally:
        obj.close()
        obj.release_conn()


def _utcnow_iso() -> str:
    return datetime.utcnow().isoformat() + "Z"


class PromotionLimitExceeded(ValueError):
    pass


class PromotionExecutionContext:
    def __init__(self, db: Session, task_id: str, artifact_id: str, domain: str, logger):
        self.db = db
        self.task_id = task_id
        self.artifact_id = artifact_id
        self.domain = domain
        self.logger = logger
        self.start_time = time.monotonic()
        self.task: Optional[Task] = None

    def elapsed_seconds(self) -> float:
        return time.monotonic() - self.start_time

    def _runtime_limit_seconds(self) -> int:
        task = self.refresh_task()
        task_limit = int(task.timeout_seconds or TASK_MAX_RUNTIME_SECONDS)
        return max(1, min(task_limit, TASK_MAX_RUNTIME_SECONDS))

    def remaining_seconds(self) -> int:
        remaining = self._runtime_limit_seconds() - self.elapsed_seconds()
        return max(1, int(math.ceil(remaining)))

    def refresh_task(self) -> Task:
        task = self.db.query(Task).filter(Task.id == self.task_id).first()
        if not task:
            raise RuntimeError("task_not_found")
        self.task = task
        return task

    def checkpoint(self, stage: str, **fields: Any) -> None:
        elapsed = round(self.elapsed_seconds(), 3)
        task = self.refresh_task()
        if task.cancel_requested:
            log_event(
                self.logger,
                "knowledge_promote_cancelled",
                task_id=self.task_id,
                artifact_id=self.artifact_id,
                domain=self.domain,
                stage=stage,
                elapsed_seconds=elapsed,
                **fields,
            )
            raise TaskCancelled()
        runtime_limit = self._runtime_limit_seconds()
        if self.elapsed_seconds() > runtime_limit:
            log_event(
                self.logger,
                "knowledge_promote_timed_out",
                task_id=self.task_id,
                artifact_id=self.artifact_id,
                domain=self.domain,
                stage=stage,
                elapsed_seconds=elapsed,
                timeout_seconds=runtime_limit,
                **fields,
            )
            raise TaskRuntimeExceeded(f"knowledge_promote_timed_out_after_{runtime_limit}s")

    def stage_event(self, event: str, **fields: Any) -> None:
        log_event(
            self.logger,
            event,
            task_id=self.task_id,
            artifact_id=self.artifact_id,
            domain=self.domain,
            elapsed_seconds=round(self.elapsed_seconds(), 3),
            **fields,
        )


def _fail_with_report(
    db: Session,
    task_id: str,
    artifact_id: str,
    domain: str,
    error_code: str,
    message: str,
    logger,
    **extra: Any,
) -> None:
    failure_payload = {
        "artifact_id": artifact_id,
        "domain": domain,
        "error_code": error_code,
        "message": message,
        "created_at": _utcnow_iso(),
        **extra,
    }
    _create_promotion_report(db, task_id, failure_payload, logger)


def _enforce_input_guardrail(ctx: PromotionExecutionContext, text: str) -> None:
    text_size = len(text.encode("utf-8"))
    if text_size > KNOWLEDGE_PROMOTION_MAX_INPUT_BYTES:
        message = (
            f"knowledge promotion input exceeds safety limit: {text_size} bytes > "
            f"{KNOWLEDGE_PROMOTION_MAX_INPUT_BYTES} bytes"
        )
        _fail_with_report(
            ctx.db,
            ctx.task_id,
            ctx.artifact_id,
            ctx.domain,
            "knowledge_promotion_input_too_large",
            message,
            ctx.logger,
            text_size_bytes=text_size,
            limit_bytes=KNOWLEDGE_PROMOTION_MAX_INPUT_BYTES,
        )
        ctx.stage_event(
            "knowledge_promote_failed",
            error="knowledge_promotion_input_too_large",
            text_size_bytes=text_size,
            limit_bytes=KNOWLEDGE_PROMOTION_MAX_INPUT_BYTES,
        )
        raise PromotionLimitExceeded(message)


def _enforce_chunk_guardrail(ctx: PromotionExecutionContext, chunks: List[Dict[str, Any]]) -> None:
    if len(chunks) > KNOWLEDGE_PROMOTION_MAX_CHUNKS:
        message = (
            f"knowledge promotion chunk count exceeds safety limit: {len(chunks)} chunks > "
            f"{KNOWLEDGE_PROMOTION_MAX_CHUNKS} chunks"
        )
        _fail_with_report(
            ctx.db,
            ctx.task_id,
            ctx.artifact_id,
            ctx.domain,
            "knowledge_promotion_too_many_chunks",
            message,
            ctx.logger,
            chunk_count=len(chunks),
            limit_chunks=KNOWLEDGE_PROMOTION_MAX_CHUNKS,
        )
        ctx.stage_event(
            "knowledge_promote_failed",
            error="knowledge_promotion_too_many_chunks",
            chunk_count=len(chunks),
            limit_chunks=KNOWLEDGE_PROMOTION_MAX_CHUNKS,
        )
        raise PromotionLimitExceeded(message)


def _batch_count(total_items: int, batch_size: int) -> int:
    if total_items <= 0:
        return 0
    return int(math.ceil(total_items / float(batch_size)))


class _AlarmTimeout:
    def __init__(self, seconds: int):
        self.seconds = max(1, int(seconds))
        self.previous_handler = None

    def _handle_timeout(self, signum, frame) -> None:
        raise TaskRuntimeExceeded(f"knowledge_promote_timed_out_after_{self.seconds}s")

    def __enter__(self):
        self.previous_handler = signal.getsignal(signal.SIGALRM)
        signal.signal(signal.SIGALRM, self._handle_timeout)
        signal.setitimer(signal.ITIMER_REAL, float(self.seconds))
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        signal.setitimer(signal.ITIMER_REAL, 0.0)
        if self.previous_handler is not None:
            signal.signal(signal.SIGALRM, self.previous_handler)


def _stringify_json(value: Any) -> str:
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, (int, float, bool)):
        return str(value)
    return json.dumps(value, sort_keys=True, ensure_ascii=False)


def _message_content_to_text(content: Any) -> str:
    if isinstance(content, str):
        return content.strip()
    if isinstance(content, dict):
        parts = content.get("parts")
        if isinstance(parts, list):
            joined = "\n".join(_stringify_json(part) for part in parts if _stringify_json(part))
            if joined.strip():
                return joined.strip()
        text = content.get("text")
        if isinstance(text, str) and text.strip():
            return text.strip()
        return _stringify_json(content)
    if isinstance(content, list):
        joined = "\n".join(_stringify_json(part) for part in content if _stringify_json(part))
        return joined.strip()
    return _stringify_json(content)


def _conversation_payload_to_text(payload: Dict[str, Any]) -> str:
    lines: List[str] = []
    title = payload.get("title")
    conversation_id = payload.get("conversation_id") or payload.get("id")
    if isinstance(title, str) and title.strip():
        lines.append(f"Title: {title.strip()}")
    if conversation_id:
        lines.append(f"Conversation ID: {conversation_id}")
    messages = payload.get("messages")
    if isinstance(messages, list):
        for message in messages:
            if not isinstance(message, dict):
                continue
            role = message.get("role") or "unknown"
            timestamp = message.get("create_time") or message.get("timestamp")
            header = f"{role}"
            if timestamp is not None:
                header = f"{header} [{timestamp}]"
            body = _message_content_to_text(message.get("content", message.get("text")))
            if body:
                lines.append(header)
                lines.append(body)
    return "\n\n".join(line for line in lines if line).strip()


def _results_payload_to_text(payload: Dict[str, Any]) -> str:
    query = payload.get("query")
    results = payload.get("results")
    lines: List[str] = []
    if isinstance(query, str) and query.strip():
        lines.append(f"Query: {query.strip()}")
    if isinstance(results, list):
        for index, result in enumerate(results, start=1):
            if not isinstance(result, dict):
                continue
            title = result.get("title")
            url = result.get("url")
            description = result.get("description") or result.get("snippet") or result.get("text")
            lines.append(f"Result {index}")
            if title:
                lines.append(f"Title: {title}")
            if url:
                lines.append(f"URL: {url}")
            if description:
                lines.append(f"Text: {_stringify_json(description)}")
    return "\n".join(line for line in lines if line).strip()


def _chunks_payload_to_text(payload: Dict[str, Any]) -> str:
    chunks = payload.get("chunks")
    if not isinstance(chunks, list):
        return ""
    lines: List[str] = []
    for chunk in chunks:
        if not isinstance(chunk, dict):
            continue
        text = chunk.get("text")
        if isinstance(text, str) and text.strip():
            lines.append(text.strip())
    return "\n\n".join(lines).strip()


def _resolve_json_text(payload: Any) -> str:
    if isinstance(payload, str):
        return payload.strip()
    if isinstance(payload, list):
        parts = []
        for item in payload:
            try:
                resolved = _resolve_json_text(item)
            except ValueError:
                continue
            if resolved:
                parts.append(resolved)
        if parts:
            return "\n\n".join(parts).strip()
        raise ValueError("knowledge_no_text")
    if not isinstance(payload, dict):
        raise ValueError("knowledge_no_text")

    if isinstance(payload.get("text"), str) and payload["text"].strip():
        return payload["text"].strip()

    chunks_text = _chunks_payload_to_text(payload)
    if chunks_text:
        return chunks_text

    if isinstance(payload.get("messages"), list):
        conversation_text = _conversation_payload_to_text(payload)
        if conversation_text:
            return conversation_text

    if isinstance(payload.get("results"), list):
        results_text = _results_payload_to_text(payload)
        if results_text:
            return results_text

    if "payload" in payload:
        return _resolve_json_text(payload["payload"])

    candidate_lines: List[str] = []
    for key in ("title", "name", "subject", "description", "summary", "body", "content"):
        value = payload.get(key)
        if isinstance(value, str) and value.strip():
            candidate_lines.append(f"{key}: {value.strip()}")
        elif key == "content" and value is not None:
            content_text = _message_content_to_text(value)
            if content_text:
                candidate_lines.append(content_text)
    if candidate_lines:
        return "\n".join(candidate_lines).strip()

    raise ValueError("knowledge_no_text")


def resolve_artifact_text(artifact: Artifact) -> str:
    body = _read_artifact_bytes(artifact)
    content_type = (artifact.content_type or "").split(";")[0].strip().lower()
    if content_type in {"text/plain", "text/markdown"} or content_type.startswith("text/"):
        text = body.decode("utf-8", errors="replace").strip()
        if not text:
            raise ValueError("knowledge_no_text")
        return text
    if content_type == "application/json":
        payload = json.loads(body.decode("utf-8"))
        text = _resolve_json_text(payload).strip()
        if not text:
            raise ValueError("knowledge_no_text")
        return text
    raise ValueError("knowledge_no_text")


def _resolve_source(request_source: Optional[str], artifact: Artifact) -> str:
    if request_source:
        return request_source
    metadata = artifact.metadata_json or {}
    if isinstance(metadata.get("source"), str) and metadata["source"].strip():
        return metadata["source"].strip()
    if artifact.type == "fetch_text":
        return "fetch"
    return "manual"


def _resolve_confidence(request_confidence: Optional[str], artifact: Artifact) -> str:
    if request_confidence:
        return request_confidence
    metadata = artifact.metadata_json or {}
    if isinstance(metadata.get("confidence"), str) and metadata["confidence"].strip():
        return metadata["confidence"].strip()
    return "medium"


def _create_promotion_report(
    db: Session,
    task_id: str,
    payload: Dict[str, Any],
    logger,
) -> Artifact:
    return create_artifact_record(
        db=db,
        task_id=task_id,
        artifact_type="knowledge_promotion_report",
        content_type="application/json",
        data=json.dumps(payload).encode("utf-8"),
        metadata=payload,
        event_logger=logger,
    )


def _create_chunks_artifact(
    db: Session,
    task_id: str,
    artifact_id: str,
    domain: str,
    chunk_params: Dict[str, int],
    chunks: List[Dict[str, Any]],
    logger,
) -> Artifact:
    payload = {
        "artifact_id": artifact_id,
        "domain": domain,
        "chunker_version": CHUNKER_VERSION,
        "chunk_params": chunk_params,
        "chunks": chunks,
    }
    return create_artifact_record(
        db=db,
        task_id=task_id,
        artifact_type="knowledge_document_chunks",
        content_type="application/json",
        data=json.dumps(payload).encode("utf-8"),
        metadata={
            "artifact_id": artifact_id,
            "domain": domain,
            "chunker_version": CHUNKER_VERSION,
            "chunk_params": chunk_params,
            "num_chunks": len(chunks),
        },
        event_logger=logger,
    )


def run_knowledge_promote_task(db: Session, task_id: str, params: Dict[str, Any], logger) -> Dict[str, Any]:
    artifact_id = str(params["artifact_id"])
    domain = KnowledgeDomain(params["domain"]).value
    ctx = PromotionExecutionContext(db=db, task_id=task_id, artifact_id=artifact_id, domain=domain, logger=logger)
    ctx.stage_event("knowledge_promote_started")
    if domain == KnowledgeDomain.BELIEF.value:
        log_event(logger, "knowledge_promote_failed", task_id=task_id, artifact_id=artifact_id, domain=domain, error="belief_blocked")
        raise PermissionError("belief promotion is blocked in this phase")

    artifact = db.query(Artifact).filter(Artifact.id == artifact_id).first()
    if not artifact:
        log_event(logger, "knowledge_promote_failed", task_id=task_id, artifact_id=artifact_id, domain=domain, error="artifact_not_found")
        raise ValueError("artifact_not_found")

    source = _resolve_source(params.get("source"), artifact)
    confidence = _resolve_confidence(params.get("confidence"), artifact)
    embedding_model = params.get("embedding_model") or EMBEDDING_MODEL_DEFAULT
    chunk_params = normalize_chunk_params(params.get("chunk_params"))
    ctx.checkpoint("before_text_resolution")

    try:
        text = resolve_artifact_text(artifact)
    except Exception as exc:
        _fail_with_report(db, task_id, artifact_id, domain, "knowledge_no_text", str(exc), logger)
        log_event(
            logger,
            "knowledge_promote_failed",
            task_id=task_id,
            artifact_id=artifact_id,
            domain=domain,
            error="knowledge_no_text",
        )
        raise ValueError("knowledge_no_text")
    ctx.stage_event("knowledge_promote_text_resolved", text_size_bytes=len(text.encode("utf-8")))
    ctx.checkpoint("after_text_resolution", text_size_bytes=len(text.encode("utf-8")))
    _enforce_input_guardrail(ctx, text)

    ctx.stage_event("knowledge_promote_embedding_provider_started", embedding_model=embedding_model)
    try:
        model_init_timeout = min(ctx.remaining_seconds(), max(1, KNOWLEDGE_PROMOTION_MODEL_INIT_TIMEOUT_SECONDS))
        with _AlarmTimeout(model_init_timeout):
            provider = get_sentence_transformers_provider(embedding_model)
    except EmbeddingProviderInitError as exc:
        _fail_with_report(
            db,
            task_id,
            artifact_id,
            domain,
            "embedding_provider_init_failed",
            str(exc),
            logger,
            embedding_model=embedding_model,
        )
        ctx.stage_event(
            "knowledge_promote_failed",
            error="embedding_provider_init_failed",
            embedding_model=embedding_model,
        )
        raise
    except TaskRuntimeExceeded as exc:
        _fail_with_report(
            db,
            task_id,
            artifact_id,
            domain,
            "embedding_provider_init_timed_out",
            str(exc),
            logger,
            embedding_model=embedding_model,
            timeout_seconds=model_init_timeout,
        )
        ctx.stage_event(
            "knowledge_promote_timed_out",
            stage="embedding_provider_init",
            embedding_model=embedding_model,
            timeout_seconds=model_init_timeout,
        )
        raise
    embedding_revision = provider.embedding_revision
    ctx.stage_event("knowledge_promote_embedding_provider_completed", embedding_model=embedding_model)
    promotion_key = compute_promotion_key(
        artifact_id=artifact_id,
        domain=domain,
        embedding_model=embedding_model,
        embedding_revision=embedding_revision,
        chunker_version=CHUNKER_VERSION,
        chunk_params=chunk_params,
    )

    existing_document = (
        db.query(KnowledgeDocument)
        .filter(KnowledgeDocument.promotion_key == promotion_key)
        .first()
    )
    if existing_document:
        existing_chunk = (
            db.query(KnowledgeChunk)
            .filter(KnowledgeChunk.document_id == existing_document.id)
            .order_by(KnowledgeChunk.chunk_index.asc())
            .first()
        )
        num_chunks = (
            db.query(KnowledgeChunk)
            .filter(KnowledgeChunk.document_id == existing_document.id)
            .count()
        )
        report_payload = {
            "artifact_id": artifact_id,
            "doc_id": existing_document.id,
            "domain": domain,
            "promotion_key": promotion_key,
            "deduped": True,
            "embedding_model": existing_document.embedding_model,
            "embedding_revision": existing_document.embedding_revision,
            "vector_dimensions": None,
            "chunker_version": existing_document.chunker_version,
            "chunk_params": existing_document.chunk_params,
            "num_chunks": num_chunks,
            "qdrant_collection": collection_name_for_domain(domain),
            "created_at": datetime.utcnow().isoformat() + "Z",
        }
        report_artifact = _create_promotion_report(db, task_id, report_payload, logger)
        artifact_ids = [report_artifact.id]
        if existing_chunk:
            artifact_ids.append(existing_chunk.text_artifact_id)
        log_event(
            logger,
            "knowledge_promote_deduped",
            task_id=task_id,
            artifact_id=artifact_id,
            domain=domain,
            promotion_key=promotion_key,
            embedding_model=existing_document.embedding_model,
            embedding_revision=existing_document.embedding_revision,
            num_chunks=num_chunks,
        )
        return {
            "artifact_ids": artifact_ids,
            "summary": f"Promotion deduped for artifact {artifact_id} in {domain}",
        }

    ctx.stage_event("knowledge_promote_chunking_started", chunk_size=chunk_params["chunk_size"], overlap=chunk_params["overlap"])
    chunks = chunk_text(
        text=text,
        chunk_size=chunk_params["chunk_size"],
        overlap=chunk_params["overlap"],
        should_continue=lambda: ctx.checkpoint("chunking"),
    )
    if not chunks:
        _fail_with_report(db, task_id, artifact_id, domain, "knowledge_no_text", "no chunks produced", logger)
        log_event(
            logger,
            "knowledge_promote_failed",
            task_id=task_id,
            artifact_id=artifact_id,
            domain=domain,
            promotion_key=promotion_key,
            embedding_model=embedding_model,
            embedding_revision=embedding_revision,
            error="knowledge_no_text",
        )
        raise ValueError("knowledge_no_text")
    _enforce_chunk_guardrail(ctx, chunks)
    ctx.stage_event("knowledge_promote_chunking_completed", chunk_count=len(chunks))
    ctx.checkpoint("after_chunking", chunk_count=len(chunks))

    embed_batch_size = max(1, KNOWLEDGE_PROMOTION_EMBED_BATCH_SIZE)
    embed_batch_count = _batch_count(len(chunks), embed_batch_size)
    vectors: List[List[float]] = []
    ctx.stage_event("knowledge_promote_embedding_started", chunk_count=len(chunks), batch_count=embed_batch_count)
    for batch_number, start_index in enumerate(range(0, len(chunks), embed_batch_size), start=1):
        ctx.checkpoint(
            "before_embedding_batch",
            batch_index=batch_number,
            batch_count=embed_batch_count,
        )
        batch = chunks[start_index:start_index + embed_batch_size]
        batch_vectors = provider.embed([str(chunk["text"]) for chunk in batch])
        vectors.extend(batch_vectors)
        ctx.stage_event(
            "knowledge_promote_embedding_batch_completed",
            batch_index=batch_number,
            batch_count=embed_batch_count,
            batch_size=len(batch),
        )
        ctx.checkpoint(
            "after_embedding_batch",
            batch_index=batch_number,
            batch_count=embed_batch_count,
        )
    vector_dimensions = len(vectors[0]) if vectors else 0
    if vector_dimensions <= 0:
        log_event(
            logger,
            "knowledge_promote_failed",
            task_id=task_id,
            artifact_id=artifact_id,
            domain=domain,
            promotion_key=promotion_key,
            embedding_model=embedding_model,
            embedding_revision=embedding_revision,
            error="embedding_empty",
        )
        raise ValueError("embedding_empty")
    ctx.stage_event("knowledge_promote_embedding_completed", chunk_count=len(chunks), vector_dimensions=vector_dimensions)

    try:
        qdrant_collection = ensure_collection(domain, vector_dimensions)
    except Exception as exc:
        ctx.stage_event(
            "knowledge_promote_failed",
            error="qdrant_unavailable",
            qdrant_error=str(exc),
            chunk_count=len(chunks),
            qdrant_collection=collection_name_for_domain(domain),
        )
        raise RuntimeError(f"qdrant_unavailable: {exc}") from exc
    document_id = str(uuid.uuid4())
    created_at = _utcnow_iso()
    point_ids: List[str] = [str(uuid.uuid4()) for _ in chunks]
    points = []
    for chunk, vector, point_id in zip(chunks, vectors, point_ids):
        points.append(
            qdrant_models.PointStruct(
                id=point_id,
                vector=vector,
                payload={
                    "doc_id": document_id,
                    "artifact_id": artifact_id,
                    "domain": domain,
                    "source": source,
                    "confidence": confidence,
                    "created_at": created_at,
                    "promotion_key": promotion_key,
                    "embedding_model": embedding_model,
                    "embedding_revision": embedding_revision,
                    "chunker_version": CHUNKER_VERSION,
                    "chunk_params": chunk_params,
                    "chunk_index": int(chunk["chunk_index"]),
                    "qdrant_point_id": point_id,
                },
            )
        )

    upsert_batch_size = max(1, KNOWLEDGE_PROMOTION_UPSERT_BATCH_SIZE)
    upsert_batch_count = _batch_count(len(points), upsert_batch_size)
    inserted_point_ids: List[str] = []
    try:
        ctx.stage_event(
            "knowledge_promote_qdrant_upsert_started",
            chunk_count=len(points),
            batch_count=upsert_batch_count,
            qdrant_collection=qdrant_collection,
        )
        for batch_number, start_index in enumerate(range(0, len(points), upsert_batch_size), start=1):
            ctx.checkpoint(
                "before_qdrant_upsert_batch",
                batch_index=batch_number,
                batch_count=upsert_batch_count,
            )
            batch = points[start_index:start_index + upsert_batch_size]
            try:
                upsert_points(domain, batch)
            except Exception as exc:
                ctx.stage_event(
                    "knowledge_promote_failed",
                    error="qdrant_unavailable",
                    qdrant_error=str(exc),
                    batch_index=batch_number,
                    batch_count=upsert_batch_count,
                    batch_size=len(batch),
                    qdrant_collection=qdrant_collection,
                )
                raise RuntimeError(f"qdrant_unavailable: {exc}") from exc
            inserted_point_ids.extend(str(point.id) for point in batch)
            ctx.stage_event(
                "knowledge_promote_qdrant_upsert_batch_completed",
                batch_index=batch_number,
                batch_count=upsert_batch_count,
                batch_size=len(batch),
            )
            ctx.checkpoint(
                "after_qdrant_upsert_batch",
                batch_index=batch_number,
                batch_count=upsert_batch_count,
            )
        ctx.stage_event(
            "knowledge_promote_qdrant_upsert_completed",
            chunk_count=len(points),
            qdrant_collection=qdrant_collection,
        )

        chunks_artifact = _create_chunks_artifact(db, task_id, artifact_id, domain, chunk_params, chunks, logger)
        document = KnowledgeDocument(
            id=document_id,
            artifact_id=artifact_id,
            domain=domain,
            source=source,
            confidence=confidence,
            promotion_key=promotion_key,
            embedding_model=embedding_model,
            embedding_revision=embedding_revision,
            chunker_version=CHUNKER_VERSION,
            chunk_params=chunk_params,
            created_at=datetime.utcnow(),
            promoted_at=datetime.utcnow(),
        )
        db.add(document)
        db.flush()
        ctx.stage_event("knowledge_promote_document_flushed", doc_id=document_id)
        for chunk, point_id in zip(chunks, point_ids):
            db.add(
                KnowledgeChunk(
                    id=str(uuid.uuid4()),
                    document_id=document_id,
                    chunk_index=int(chunk["chunk_index"]),
                    qdrant_point_id=point_id,
                    text_artifact_id=chunks_artifact.id,
                    created_at=datetime.utcnow(),
                )
            )
        db.commit()
    except Exception as exc:
        db.rollback()
        if inserted_point_ids:
            try:
                delete_points(domain, inserted_point_ids)
            except Exception as cleanup_exc:
                ctx.stage_event(
                    "knowledge_promote_failed",
                    error="qdrant_cleanup_failed",
                    cleanup_error=str(cleanup_exc),
                    cleaned_point_count=len(inserted_point_ids),
                )
        if not isinstance(exc, (TaskCancelled, TaskRuntimeExceeded)):
            ctx.stage_event(
                "knowledge_promote_failed",
                error=str(exc),
                chunk_count=len(points),
                qdrant_collection=qdrant_collection,
            )
        raise

    report_payload = {
        "artifact_id": artifact_id,
        "doc_id": document_id,
        "domain": domain,
        "promotion_key": promotion_key,
        "deduped": False,
        "embedding_model": embedding_model,
        "embedding_revision": embedding_revision,
        "vector_dimensions": vector_dimensions,
        "chunker_version": CHUNKER_VERSION,
        "chunk_params": chunk_params,
        "num_chunks": len(chunks),
        "qdrant_collection": qdrant_collection,
        "created_at": _utcnow_iso(),
    }
    report_artifact = _create_promotion_report(db, task_id, report_payload, logger)

    log_event(
        logger,
        "knowledge_promoted",
        task_id=task_id,
        artifact_id=artifact_id,
        domain=domain,
        promotion_key=promotion_key,
        embedding_model=embedding_model,
        embedding_revision=embedding_revision,
        num_chunks=len(chunks),
    )
    return {
        "artifact_ids": [report_artifact.id, chunks_artifact.id],
        "summary": f"Promoted artifact {artifact_id} to {domain} using {embedding_model} ({len(chunks)} chunks)",
    }
