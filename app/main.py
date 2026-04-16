import time
import uuid
from hashlib import sha256
from datetime import datetime
from typing import Any, Dict, Optional

from fastapi import Depends, FastAPI, File, HTTPException, Query, Request, UploadFile, status
from pydantic import BaseModel, Field
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.db import check_db, get_db, init_db
from app.artifacts import router as artifacts_router
from app.imports import delete_staged_import, detect_upload_mime, read_upload_limited, stage_import_bytes
from app.knowledge.chunking import normalize_chunk_params
from app.knowledge.domains import KnowledgeDomain
from app.knowledge.query import QUERY_DOMAIN_MODES, normalize_domains, normalize_query_filters
from app.logging_utils import configure_logging, get_logger, log_event, log_task_transition
from app.limits import (
    CHUNK_OVERLAP,
    CHUNK_SIZE,
    EMBEDDING_MODEL_DEFAULT,
    FETCH_DOMAIN_ALLOWLIST,
    KNOWLEDGE_QUERY_MAX_DOMAINS,
    KNOWLEDGE_PROMOTION_EMBED_BATCH_SIZE,
    KNOWLEDGE_PROMOTION_MAX_CHUNKS,
    KNOWLEDGE_PROMOTION_MAX_INPUT_BYTES,
    KNOWLEDGE_PROMOTION_MODEL_INIT_TIMEOUT_SECONDS,
    KNOWLEDGE_PROMOTION_UPSERT_BATCH_SIZE,
    MAX_BATCH_SIZE,
    MAX_IMPORT_FILE_MB,
    QDRANT_HEALTH_TIMEOUT_SECONDS,
    SEARCH_SOURCE_ALLOWLIST,
    TASK_MAX_ARTIFACT_MB,
    TASK_MAX_RUNTIME_SECONDS,
    TASKS_PER_MINUTE,
    TOP_K_PER_DOMAIN,
    rate_limit_check,
    search_sources_allowed,
    validate_fetch_domain,
)
from app.models import Task
from app.queue import enqueue_task, get_redis, get_queue_for_task, WORKER_HEARTBEAT_KEY
from app.storage import ensure_bucket, get_client
from app.search import EXA_API_KEY, normalize_batch_params
from app.fetch import canonicalize_url, normalize_fetch_params
from app.knowledge.qdrant import close_qdrant_client, get_qdrant_client

configure_logging()
logger = get_logger("api")

app = FastAPI()
app.include_router(artifacts_router)


class TaskCreateRequest(BaseModel):
    type: str
    params: Dict[str, Any] = Field(default_factory=dict)
    idempotency_key: Optional[str] = None
    timeout_seconds: int = 300
    max_retries: int = 0


class TaskCreateResponse(BaseModel):
    task_id: str
    status: str


class TaskStatusResponse(BaseModel):
    task_id: str
    status: str
    type: str
    created_at: str
    updated_at: str
    started_at: Optional[str]
    finished_at: Optional[str]
    result: Optional[Dict[str, Any]]
    error: Optional[str]


class TaskListItemResponse(BaseModel):
    id: str
    task_type: str
    status: str
    created_at: str
    updated_at: str
    started_at: Optional[str]
    finished_at: Optional[str]
    summary: Optional[str]
    error: Optional[str]


class TaskListResponse(BaseModel):
    total: int
    limit: int
    offset: int
    tasks: list[TaskListItemResponse]


class SearchRequest(BaseModel):
    query: str
    sources: Optional[list[str]] = None
    recency_days: Optional[int] = None


class SearchBatchRequest(BaseModel):
    queries: list[str]
    sources: Optional[list[str]] = None
    recency_days: Optional[int] = None


class FetchRequest(BaseModel):
    url: str
    reader_mode: Optional[bool] = True
    store_raw_html: Optional[bool] = False


class KnowledgePromoteRequest(BaseModel):
    artifact_id: str
    domain: KnowledgeDomain
    source: Optional[str] = None
    confidence: Optional[str] = None
    embedding_model: Optional[str] = None
    chunk_params: Optional[Dict[str, int]] = None


class KnowledgeQueryFilters(BaseModel):
    source: Optional[list[str]] = None
    confidence_min: Optional[str] = None


class KnowledgeQueryRequest(BaseModel):
    query: str
    domains: Optional[list[KnowledgeDomain]] = None
    domain_mode: Optional[str] = None
    top_k_per_domain: Optional[int] = None
    filters: Optional[KnowledgeQueryFilters] = None


def _import_max_bytes() -> int:
    return MAX_IMPORT_FILE_MB * 1024 * 1024


@app.on_event("startup")
def on_startup() -> None:
    init_db()
    try:
        ensure_bucket(get_client())
    except Exception:
        pass
    log_event(logger, "api_startup")


def create_task_record(
    db: Session,
    task_type: str,
    params: Dict[str, Any],
    idempotency_key: Optional[str],
    timeout_seconds: int,
    max_retries: int,
    allowed_types: Optional[set[str]] = None,
) -> TaskCreateResponse:
    allowed, current = rate_limit_check()
    if not allowed:
        log_event(
            logger,
            "rate_limit_exceeded",
            reason="rate_limit_exceeded",
            limit=TASKS_PER_MINUTE,
            current=current,
        )
        raise HTTPException(status_code=429, detail="rate_limit_exceeded")
    if allowed_types is None:
        allowed_types = {"echo", "sleep", "search"}
    if task_type not in allowed_types:
        raise HTTPException(status_code=400, detail="unsupported task type")

    if task_type == "sleep":
        seconds = params.get("seconds")
        if seconds is None or not isinstance(seconds, int) or seconds < 0:
            raise HTTPException(status_code=400, detail="sleep requires params.seconds as a non-negative integer")

    if task_type == "search":
        query = params.get("query")
        if query is None or not isinstance(query, str) or not query.strip():
            raise HTTPException(status_code=400, detail="search requires params.query as a non-empty string")
        sources = params.get("sources") or ["brave"]
        if not isinstance(sources, list) or not sources:
            log_event(logger, "task_rejected_invalid_params", reason="search_sources_invalid")
            raise HTTPException(status_code=400, detail="search requires params.sources as a non-empty list")

    if idempotency_key:
        existing = (
            db.query(Task)
            .filter(Task.idempotency_key == idempotency_key)
            .first()
        )
        if existing:
            if existing.type != task_type:
                raise HTTPException(status_code=409, detail="idempotency_key already used for different task type")
            return TaskCreateResponse(task_id=existing.id, status=existing.status)

    task_id = str(uuid.uuid4())
    now = datetime.utcnow()
    task = Task(
        id=task_id,
        type=task_type,
        status="queued",
        created_at=now,
        updated_at=now,
        idempotency_key=idempotency_key,
        params_json=params,
        result_json=None,
        error=None,
        timeout_seconds=timeout_seconds,
        max_retries=max_retries,
        retry_count=0,
        started_at=None,
        finished_at=None,
        cancel_requested=False,
    )
    db.add(task)
    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        existing = (
            db.query(Task)
            .filter(Task.idempotency_key == idempotency_key)
            .first()
        )
        if existing and existing.type == task_type:
            return TaskCreateResponse(task_id=existing.id, status=existing.status)
        raise HTTPException(status_code=409, detail="idempotency_key already exists")

    queue_name = get_queue_for_task(task_type)
    enqueue_task(task_id, task_type)
    log_event(logger, "task_enqueued", task_id=task_id, task_type=task_type, queue=queue_name)
    log_task_transition(logger, task_id, task_type, "new", "queued")
    return TaskCreateResponse(task_id=task_id, status="queued")


@app.post("/tasks", response_model=TaskCreateResponse, status_code=status.HTTP_202_ACCEPTED)
def create_task(payload: TaskCreateRequest, db: Session = Depends(get_db)) -> TaskCreateResponse:
    return create_task_record(
        db=db,
        task_type=payload.type,
        params=payload.params,
        idempotency_key=payload.idempotency_key,
        timeout_seconds=payload.timeout_seconds,
        max_retries=payload.max_retries,
    )


@app.post("/search", response_model=TaskCreateResponse, status_code=status.HTTP_202_ACCEPTED)
def submit_search(payload: SearchRequest, db: Session = Depends(get_db)) -> TaskCreateResponse:
    params: Dict[str, Any] = {
        "query": payload.query,
        "sources": payload.sources,
        "recency_days": payload.recency_days,
    }
    params = {k: v for k, v in params.items() if v is not None}
    sources = params.get("sources") or ["brave"]
    if isinstance(sources, list) and "exa" in [source.lower() for source in sources] and not EXA_API_KEY:
        log_event(logger, "task_rejected_invalid_params", reason="exa_api_key_missing")
        raise HTTPException(status_code=400, detail="exa api key missing")
    return create_task_record(
        db=db,
        task_type="search",
        params=params,
        idempotency_key=None,
        timeout_seconds=300,
        max_retries=0,
    )


@app.post("/search/batch", response_model=TaskCreateResponse, status_code=status.HTTP_202_ACCEPTED)
def submit_search_batch(payload: SearchBatchRequest, db: Session = Depends(get_db)) -> TaskCreateResponse:
    try:
        normalized = normalize_batch_params(payload.dict())
    except Exception as exc:
        log_event(logger, "task_rejected_invalid_params", reason=str(exc))
        raise HTTPException(status_code=400, detail="invalid batch search params")
    if len(normalized["queries"]) > MAX_BATCH_SIZE:
        log_event(logger, "task_rejected_invalid_params", reason="batch_size_exceeded", limit=MAX_BATCH_SIZE)
        raise HTTPException(status_code=400, detail="batch size exceeded")
    if not search_sources_allowed(normalized["sources"]):
        log_event(logger, "task_rejected_invalid_params", reason="search_source_not_allowed")
        raise HTTPException(status_code=400, detail="search sources not allowed")
    if "exa" in [source.lower() for source in normalized["sources"]] and not EXA_API_KEY:
        log_event(logger, "task_rejected_invalid_params", reason="exa_api_key_missing")
        raise HTTPException(status_code=400, detail="exa api key missing")

    return create_task_record(
        db=db,
        task_type="search_batch",
        params=normalized,
        idempotency_key=None,
        timeout_seconds=300,
        max_retries=0,
        allowed_types={"search_batch"},
    )


@app.post("/fetch", response_model=TaskCreateResponse, status_code=status.HTTP_202_ACCEPTED)
def submit_fetch(payload: FetchRequest, db: Session = Depends(get_db)) -> TaskCreateResponse:
    try:
        canonical_url = canonicalize_url(payload.url)
    except Exception:
        log_event(logger, "task_rejected_invalid_params", reason="invalid_url")
        raise HTTPException(status_code=400, detail="invalid url")
    if not validate_fetch_domain(canonical_url):
        log_event(logger, "task_rejected_invalid_params", reason="fetch_domain_not_allowed")
        raise HTTPException(status_code=400, detail="fetch domain not allowed")
    params: Dict[str, Any] = {
        "url": canonical_url,
        "reader_mode": bool(payload.reader_mode),
        "store_raw_html": bool(payload.store_raw_html),
    }
    # Normalize once to ensure deterministic parameters.
    try:
        normalize_fetch_params(params)
    except Exception:
        log_event(logger, "task_rejected_invalid_params", reason="invalid_fetch_params")
        raise HTTPException(status_code=400, detail="invalid fetch params")
    return create_task_record(
        db=db,
        task_type="fetch_url",
        params=params,
        idempotency_key=None,
        timeout_seconds=300,
        max_retries=0,
        allowed_types={"fetch_url"},
    )


@app.post("/import/files", response_model=TaskCreateResponse, status_code=status.HTTP_202_ACCEPTED)
async def submit_file_import(file: UploadFile = File(...), db: Session = Depends(get_db)) -> TaskCreateResponse:
    if not file.filename:
        raise HTTPException(status_code=400, detail="filename required")

    raw_bytes = await read_upload_limited(file, _import_max_bytes())
    mime = detect_upload_mime(file.filename, file.content_type)

    suffix = ""
    if "." in file.filename:
        suffix = f".{file.filename.rsplit('.', 1)[-1]}"
    staging_path = stage_import_bytes("files", raw_bytes, mime, suffix=suffix)
    params = {
        "staging_path": staging_path,
        "filename": file.filename,
        "mime": mime,
        "size_bytes": len(raw_bytes),
        "sha256": sha256(raw_bytes).hexdigest(),
        "source": "files",
    }
    try:
        response = create_task_record(
            db=db,
            task_type="import_file",
            params=params,
            idempotency_key=None,
            timeout_seconds=300,
            max_retries=0,
            allowed_types={"import_file"},
        )
    except Exception:
        delete_staged_import(staging_path)
        raise
    log_event(logger, "import_requested", task_id=response.task_id, artifact_ids=[], source="files")
    return response


@app.post("/import/chatgpt", response_model=TaskCreateResponse, status_code=status.HTTP_202_ACCEPTED)
async def submit_chatgpt_import(request: Request, db: Session = Depends(get_db)) -> TaskCreateResponse:
    content_type = request.headers.get("content-type", "")
    if "application/json" not in content_type.lower():
        raise HTTPException(status_code=400, detail="content type must be application/json")

    raw_bytes = await request.body()
    if len(raw_bytes) > _import_max_bytes():
        raise HTTPException(status_code=413, detail="payload too large")
    try:
        await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="invalid json payload")

    staging_path = stage_import_bytes("chatgpt", raw_bytes, "application/json", suffix=".json")
    params = {
        "staging_path": staging_path,
        "sha256": sha256(raw_bytes).hexdigest(),
        "size_bytes": len(raw_bytes),
        "source": "chatgpt",
    }
    try:
        response = create_task_record(
            db=db,
            task_type="import_chatgpt",
            params=params,
            idempotency_key=None,
            timeout_seconds=300,
            max_retries=0,
            allowed_types={"import_chatgpt"},
        )
    except Exception:
        delete_staged_import(staging_path)
        raise
    log_event(logger, "import_requested", task_id=response.task_id, artifact_ids=[], source="chatgpt")
    return response


@app.post("/knowledge/promote", response_model=TaskCreateResponse, status_code=status.HTTP_202_ACCEPTED)
def submit_knowledge_promote(payload: KnowledgePromoteRequest, db: Session = Depends(get_db)) -> TaskCreateResponse:
    if payload.domain == KnowledgeDomain.BELIEF:
        raise HTTPException(status_code=403, detail="belief promotion is blocked in this phase")

    try:
        chunk_params = normalize_chunk_params(payload.chunk_params)
    except Exception as exc:
        log_event(logger, "task_rejected_invalid_params", reason=str(exc))
        raise HTTPException(status_code=400, detail="invalid chunk params")

    embedding_model = payload.embedding_model or EMBEDDING_MODEL_DEFAULT
    params = {
        "artifact_id": payload.artifact_id,
        "domain": payload.domain.value,
        "source": payload.source,
        "confidence": payload.confidence,
        "embedding_model": embedding_model,
        "chunk_params": chunk_params,
    }
    params = {key: value for key, value in params.items() if value is not None}
    response = create_task_record(
        db=db,
        task_type="knowledge_promote",
        params=params,
        idempotency_key=None,
        timeout_seconds=300,
        max_retries=0,
        allowed_types={"knowledge_promote"},
    )
    log_event(
        logger,
        "knowledge_promote_requested",
        task_id=response.task_id,
        artifact_id=payload.artifact_id,
        domain=payload.domain.value,
        embedding_model=embedding_model,
    )
    return response


@app.post("/knowledge/query", response_model=TaskCreateResponse, status_code=status.HTTP_202_ACCEPTED)
def submit_knowledge_query(payload: KnowledgeQueryRequest, db: Session = Depends(get_db)) -> TaskCreateResponse:
    query = " ".join(payload.query.split()).strip()
    if not query:
        raise HTTPException(status_code=400, detail="query is required")

    requested_domains = [domain.value for domain in payload.domains] if payload.domains else None
    if requested_domains:
        requested_domains = normalize_domains(requested_domains)

    domain_mode = payload.domain_mode
    if requested_domains and domain_mode is None:
        domain_mode = "prefer"
    if domain_mode is not None and domain_mode not in QUERY_DOMAIN_MODES:
        raise HTTPException(status_code=400, detail="invalid domain_mode")

    top_k_per_domain = payload.top_k_per_domain or TOP_K_PER_DOMAIN
    if top_k_per_domain <= 0:
        raise HTTPException(status_code=400, detail="invalid top_k_per_domain")

    try:
        filters = normalize_query_filters(payload.filters.model_dump(exclude_none=True) if payload.filters else None)
    except Exception as exc:
        log_event(logger, "task_rejected_invalid_params", reason=str(exc))
        raise HTTPException(status_code=400, detail="invalid filters")

    params: Dict[str, Any] = {
        "query": query,
        "top_k_per_domain": top_k_per_domain,
        "filters": filters,
    }
    if requested_domains:
        params["domains"] = requested_domains
    if domain_mode is not None:
        params["domain_mode"] = domain_mode

    response = create_task_record(
        db=db,
        task_type="knowledge_query",
        params=params,
        idempotency_key=None,
        timeout_seconds=300,
        max_retries=0,
        allowed_types={"knowledge_query"},
    )
    log_event(
        logger,
        "knowledge_query_requested",
        task_id=response.task_id,
        query=query,
        domains_requested=requested_domains or [],
        top_k_per_domain=top_k_per_domain,
    )
    return response


@app.get("/tasks/{task_id}", response_model=TaskStatusResponse)
def get_task(task_id: str, db: Session = Depends(get_db)) -> TaskStatusResponse:
    task = db.query(Task).filter(Task.id == task_id).first()
    if not task:
        raise HTTPException(status_code=404, detail="task not found")

    return TaskStatusResponse(
        task_id=task.id,
        status=task.status,
        type=task.type,
        created_at=task.created_at.isoformat() + "Z",
        updated_at=task.updated_at.isoformat() + "Z",
        started_at=task.started_at.isoformat() + "Z" if task.started_at else None,
        finished_at=task.finished_at.isoformat() + "Z" if task.finished_at else None,
        result=task.result_json,
        error=task.error,
    )


@app.get("/tasks", response_model=TaskListResponse)
def list_tasks(
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    status_filter: Optional[str] = Query(None, alias="status"),
    task_type: Optional[str] = Query(None),
    db: Session = Depends(get_db),
) -> TaskListResponse:
    query = db.query(Task)
    if status_filter:
        query = query.filter(Task.status == status_filter)
    if task_type:
        query = query.filter(Task.type == task_type)

    total = query.count()
    tasks = (
        query
        .order_by(Task.created_at.desc())
        .offset(offset)
        .limit(limit)
        .all()
    )

    items = [
        TaskListItemResponse(
            id=task.id,
            task_type=task.type,
            status=task.status,
            created_at=task.created_at.isoformat() + "Z",
            updated_at=task.updated_at.isoformat() + "Z",
            started_at=task.started_at.isoformat() + "Z" if task.started_at else None,
            finished_at=task.finished_at.isoformat() + "Z" if task.finished_at else None,
            summary=(task.result_json or {}).get("summary") if isinstance(task.result_json, dict) else None,
            error=task.error,
        )
        for task in tasks
    ]

    return TaskListResponse(total=total, limit=limit, offset=offset, tasks=items)


@app.post("/tasks/{task_id}/cancel")
def cancel_task(task_id: str, db: Session = Depends(get_db)) -> Dict[str, Any]:
    task = db.query(Task).filter(Task.id == task_id).first()
    if not task:
        raise HTTPException(status_code=404, detail="task not found")

    if task.status in {"completed", "failed", "cancelled"}:
        return {"task_id": task.id, "status": task.status}

    task.cancel_requested = True
    task.updated_at = datetime.utcnow()
    db.commit()
    log_event(logger, "task_cancel_requested", task_id=task.id, type=task.type, status=task.status)
    return {"task_id": task.id, "status": "cancel_requested"}


@app.get("/health")
def health() -> Dict[str, Any]:
    redis_status = "down"
    db_status = "down"
    worker_status = "unknown"
    minio_status = "down"
    qdrant_status = "down"

    try:
        r = get_redis()
        r.ping()
        redis_status = "ok"
        heartbeat = r.get(WORKER_HEARTBEAT_KEY)
        if heartbeat:
            age = time.time() - float(heartbeat)
            worker_status = "ok" if age < 15 else "unknown"
    except Exception:
        redis_status = "down"

    try:
        db_status = "ok" if check_db() else "down"
    except Exception:
        db_status = "down"

    try:
        client = get_client()
        ensure_bucket(client)
        minio_status = "ok"
    except Exception:
        minio_status = "down"

    try:
        qdrant_client = get_qdrant_client(timeout=QDRANT_HEALTH_TIMEOUT_SECONDS)
        try:
            qdrant_client.get_collections()
        finally:
            close_qdrant_client(qdrant_client)
        qdrant_status = "ok"
    except Exception:
        qdrant_status = "down"

    return {
        "ok": all(
            status == "ok"
            for status in (redis_status, db_status, worker_status, minio_status, qdrant_status)
        ),
        "redis": redis_status,
        "db": db_status,
        "worker": worker_status,
        "minio": minio_status,
        "qdrant": qdrant_status,
    }


@app.get("/limits")
def get_limits() -> Dict[str, Any]:
    return {
        "task_max_runtime_seconds": TASK_MAX_RUNTIME_SECONDS,
        "task_max_artifact_mb": TASK_MAX_ARTIFACT_MB,
        "tasks_per_minute": TASKS_PER_MINUTE,
        "search_source_allowlist": SEARCH_SOURCE_ALLOWLIST,
        "fetch_domain_allowlist": FETCH_DOMAIN_ALLOWLIST,
        "max_batch_size": MAX_BATCH_SIZE,
        "max_import_file_mb": MAX_IMPORT_FILE_MB,
        "embedding_model_default": EMBEDDING_MODEL_DEFAULT,
        "chunk_size": CHUNK_SIZE,
        "chunk_overlap": CHUNK_OVERLAP,
        "top_k_per_domain": TOP_K_PER_DOMAIN,
        "knowledge_query_max_domains": KNOWLEDGE_QUERY_MAX_DOMAINS,
        "knowledge_promotion_max_input_bytes": KNOWLEDGE_PROMOTION_MAX_INPUT_BYTES,
        "knowledge_promotion_max_chunks": KNOWLEDGE_PROMOTION_MAX_CHUNKS,
        "knowledge_promotion_embed_batch_size": KNOWLEDGE_PROMOTION_EMBED_BATCH_SIZE,
        "knowledge_promotion_upsert_batch_size": KNOWLEDGE_PROMOTION_UPSERT_BATCH_SIZE,
        "knowledge_promotion_model_init_timeout_seconds": KNOWLEDGE_PROMOTION_MODEL_INIT_TIMEOUT_SECONDS,
    }
