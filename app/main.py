import time
import uuid
from datetime import datetime
from typing import Any, Dict, Optional

from fastapi import Depends, FastAPI, HTTPException, status
from pydantic import BaseModel, Field
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.db import check_db, get_db, init_db
from app.artifacts import router as artifacts_router
from app.logging_utils import configure_logging, get_logger, log_event, log_task_transition
from app.limits import (
    FETCH_DOMAIN_ALLOWLIST,
    MAX_BATCH_SIZE,
    SEARCH_SOURCE_ALLOWLIST,
    TASK_MAX_ARTIFACT_MB,
    TASK_MAX_RUNTIME_SECONDS,
    TASKS_PER_MINUTE,
    rate_limit_check,
    search_sources_allowed,
)
from app.models import Task
from app.queue import enqueue_task, get_redis, WORKER_HEARTBEAT_KEY
from app.storage import ensure_bucket, get_client
from app.search import normalize_batch_params

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


class SearchRequest(BaseModel):
    query: str
    sources: Optional[list[str]] = None
    recency_days: Optional[int] = None


class SearchBatchRequest(BaseModel):
    queries: list[str]
    sources: Optional[list[str]] = None
    recency_days: Optional[int] = None


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

    enqueue_task(task_id)
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

    return create_task_record(
        db=db,
        task_type="search_batch",
        params=normalized,
        idempotency_key=None,
        timeout_seconds=300,
        max_retries=0,
        allowed_types={"search_batch"},
    )


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

    return {"ok": redis_status == "ok" and db_status == "ok", "redis": redis_status, "db": db_status, "worker": worker_status}


@app.get("/limits")
def get_limits() -> Dict[str, Any]:
    return {
        "task_max_runtime_seconds": TASK_MAX_RUNTIME_SECONDS,
        "task_max_artifact_mb": TASK_MAX_ARTIFACT_MB,
        "tasks_per_minute": TASKS_PER_MINUTE,
        "search_source_allowlist": SEARCH_SOURCE_ALLOWLIST,
        "fetch_domain_allowlist": FETCH_DOMAIN_ALLOWLIST,
        "max_batch_size": MAX_BATCH_SIZE,
    }
