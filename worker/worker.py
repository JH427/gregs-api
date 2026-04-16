import hashlib
import json
import os
import threading
import time
from datetime import datetime
from typing import Optional

import redis
from sqlalchemy.orm import Session

from app.artifacts import create_artifact_record
from app.db import SessionLocal, init_db
from app.db_helpers import (
    create_metric,
    create_run,
    has_been_processed,
    latest_run_for_key,
    update_run_status,
)
from app.logging_utils import configure_logging, get_logger, log_event, log_task_transition
from app.limits import (
    EMBEDDING_MODEL_DEFAULT,
    MAX_BATCH_SIZE,
    SEARCH_SOURCE_ALLOWLIST,
    TASK_MAX_RUNTIME_SECONDS,
    search_sources_allowed,
    validate_fetch_domain,
)
from app.models import Task
from app.queue import (
    ack_processing,
    ack_task,
    dequeue_task,
    ensure_enqueued,
    get_processing_name,
    get_queue_for_task,
    get_redis,
    get_queue_set,
    requeue_inflight,
    update_worker_heartbeat,
)
from app.search import (
    EXA_API_KEY,
    TaskCancelled,
    TaskRuntimeExceeded,
    normalize_batch_params,
    normalize_params,
    run_search_batch_task,
    run_search_task,
)
from app.fetch import normalize_fetch_params, run_fetch_task
from app.imports import run_import_chatgpt_task, run_import_file_task
from app.knowledge.embeddings import get_sentence_transformers_provider
from app.knowledge.query import run_knowledge_query_task
from app.knowledge.promotion import run_knowledge_promote_task
from app.storage import ensure_bucket, get_client

POLL_TIMEOUT = int(os.getenv("WORKER_POLL_TIMEOUT", "5"))
REDIS_RETRY_DELAY = float(os.getenv("REDIS_RETRY_DELAY", "2"))
RECONCILE_INTERVAL = float(os.getenv("QUEUE_RECONCILE_INTERVAL", "10"))
WORKER_QUEUE = os.getenv("WORKER_QUEUE", "queue_query")
VALID_WORKER_QUEUES = {"queue_query", "queue_promote"}

configure_logging()
logger = get_logger("worker")


def task_belongs_to_worker(task: Task) -> bool:
    return get_queue_for_task(task.type) == WORKER_QUEUE


def heartbeat_loop() -> None:
    while True:
        try:
            update_worker_heartbeat()
        except Exception:
            pass
        time.sleep(5)


def preload_embedding_provider() -> None:
    started_at = time.time()
    log_event(logger, "embedding_model_load_started", model_name=EMBEDDING_MODEL_DEFAULT)
    try:
        provider = get_sentence_transformers_provider(EMBEDDING_MODEL_DEFAULT)
    except Exception as exc:
        log_event(
            logger,
            "embedding_model_load_failed",
            model_name=EMBEDDING_MODEL_DEFAULT,
            elapsed_seconds=round(time.time() - started_at, 3),
            error=str(exc),
        )
        return
    load_elapsed = round(time.time() - started_at, 3)
    log_event(
        logger,
        "embedding_model_load_completed",
        model_name=EMBEDDING_MODEL_DEFAULT,
        embedding_revision=provider.embedding_revision,
        elapsed_seconds=load_elapsed,
    )
    warmup_started_at = time.time()
    log_event(logger, "embedding_model_warmup_started", model_name=EMBEDDING_MODEL_DEFAULT)
    try:
        provider.embed(["warmup"])
    except Exception as exc:
        log_event(
            logger,
            "embedding_model_warmup_failed",
            model_name=EMBEDDING_MODEL_DEFAULT,
            embedding_revision=provider.embedding_revision,
            elapsed_seconds=round(time.time() - warmup_started_at, 3),
            error=str(exc),
        )
        return
    warmup_elapsed = round(time.time() - warmup_started_at, 3)
    log_event(
        logger,
        "embedding_model_warmup_completed",
        model_name=EMBEDDING_MODEL_DEFAULT,
        embedding_revision=provider.embedding_revision,
        elapsed_seconds=warmup_elapsed,
    )
    log_event(
        logger,
        "embedding_provider_ready",
        model_name=EMBEDDING_MODEL_DEFAULT,
        embedding_revision=provider.embedding_revision,
        load_elapsed_seconds=load_elapsed,
        warmup_elapsed_seconds=warmup_elapsed,
    )


def transition_status(db: Session, task: Task, to_status: str, **fields) -> None:
    from_status = task.status
    task.status = to_status
    task.updated_at = datetime.utcnow()
    db.commit()
    log_task_transition(logger, task.id, task.type, from_status, to_status, **fields)


def mark_cancelled(db: Session, task: Task) -> None:
    task.finished_at = datetime.utcnow()
    transition_status(db, task, "cancelled")


def mark_failed(db: Session, task: Task, error: str) -> None:
    task.error = error
    task.finished_at = datetime.utcnow()
    transition_status(db, task, "failed", error=error)


def connect_redis() -> redis.Redis:
    while True:
        try:
            r = get_redis()
            r.ping()
            return r
        except Exception as exc:
            log_event(logger, "redis_connect_failed", error=str(exc))
            time.sleep(REDIS_RETRY_DELAY)


def recover_queue_state(r: redis.Redis, db: Session) -> None:
    moved = requeue_inflight(r, WORKER_QUEUE)
    if moved:
        log_event(
            logger,
            "requeued_inflight",
            count=moved,
            from_queue=get_processing_name(WORKER_QUEUE),
            to_queue=WORKER_QUEUE,
        )

    running = db.query(Task).filter(Task.status == "running").all()
    for task in running:
        if not task_belongs_to_worker(task):
            continue
        from_status = task.status
        task.status = "queued"
        task.started_at = None
        task.updated_at = datetime.utcnow()
        db.commit()
        log_task_transition(logger, task.id, task.type, from_status, "queued", reason="worker_recovery")

    queued = db.query(Task).filter(Task.status == "queued").all()
    for task in queued:
        if not task_belongs_to_worker(task):
            continue
        ensure_enqueued(r, task.id, task.type)


def ensure_queued_tasks(r: redis.Redis, db: Session) -> None:
    queued = db.query(Task).filter(Task.status == "queued").all()
    for task in queued:
        if not task_belongs_to_worker(task):
            continue
        ensure_enqueued(r, task.id, task.type)


def refresh_task(db: Session, task: Task) -> Task:
    db.refresh(task)
    return task


def _normalize_params_for_task(task: Task) -> dict:
    if task.type == "search":
        try:
            return normalize_params(task.params_json)
        except Exception:
            return task.params_json or {}
    if task.type == "search_batch":
        try:
            return normalize_batch_params(task.params_json)
        except Exception:
            return task.params_json or {}
    if task.type == "fetch_url":
        try:
            return normalize_fetch_params(task.params_json)
        except Exception:
            return task.params_json or {}
    if task.type == "import_file":
        return {
            "source": task.params_json.get("source"),
            "filename": task.params_json.get("filename"),
            "mime": task.params_json.get("mime"),
            "sha256": task.params_json.get("sha256"),
        }
    if task.type == "import_chatgpt":
        return {
            "source": task.params_json.get("source"),
            "sha256": task.params_json.get("sha256"),
        }
    if task.type == "knowledge_promote":
        return {"task_id": task.id}
    if task.type == "knowledge_query":
        return {"task_id": task.id}
    if task.type == "sleep":
        return {"seconds": int(task.params_json.get("seconds", 0))}
    return task.params_json or {}


def _processing_key(task: Task) -> str:
    normalized = _normalize_params_for_task(task)
    payload = json.dumps(normalized, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()
    return f"{task.type}:{digest}"


def execute_task(db: Session, task: Task) -> bool:
    task = refresh_task(db, task)
    if task.cancel_requested:
        mark_cancelled(db, task)
        return False

    from_status = task.status
    task.status = "running"
    task.started_at = datetime.utcnow()
    task.retry_count += 1
    db.commit()
    log_task_transition(logger, task.id, task.type, from_status, "running", retry_count=task.retry_count)

    start_time = time.time()
    processing_key = _processing_key(task)
    log_event(logger, "dedupe_key_used", task_id=task.id, processing_key=processing_key)

    processed, _ = has_been_processed(db, key=processing_key)
    if processed:
        run = latest_run_for_key(db, processing_key)
        artifact_ids = []
        summary = "deduplicated"
        if run and run.metadata_json:
            original_task_id = run.metadata_json.get("task_id")
            if original_task_id:
                original = db.query(Task).filter(Task.id == original_task_id).first()
                if original and isinstance(original.result_json, dict):
                    artifact_ids = original.result_json.get("artifact_ids") or []
                    original_summary = original.result_json.get("summary")
                    if original_summary:
                        summary = f"deduplicated; {original_summary}"
        if run:
            summary = f"{summary} (run {run.id})"
        task.result_json = {"artifact_ids": artifact_ids, "summary": summary}
        task.error = None
        task.finished_at = datetime.utcnow()
        transition_status(db, task, "completed")
        log_event(logger, "task_deduplicated", task_id=task.id, processing_key=processing_key)
        return False

    run = create_run(db, run_key=processing_key, metadata_json={"task_id": task.id, "task_type": task.type})
    try:
        if time.time() - start_time > TASK_MAX_RUNTIME_SECONDS:
            log_event(logger, "task_killed", task_id=task.id, reason="runtime_limit", limit=TASK_MAX_RUNTIME_SECONDS)
            mark_failed(db, task, "task_runtime_exceeded")
            update_run_status(db, run, "failed")
            return False
        if task.type == "echo":
            if time.time() - start_time > task.timeout_seconds:
                raise TimeoutError("task timed out")
            if time.time() - start_time > TASK_MAX_RUNTIME_SECONDS:
                log_event(logger, "task_killed", task_id=task.id, reason="runtime_limit", limit=TASK_MAX_RUNTIME_SECONDS)
                mark_failed(db, task, "task_runtime_exceeded")
                update_run_status(db, run, "failed")
                return False
            task = refresh_task(db, task)
            if task.cancel_requested:
                mark_cancelled(db, task)
                update_run_status(db, run, "failed")
                create_metric(
                    db,
                    name="duration_ms",
                    value=int((time.time() - start_time) * 1000),
                    run_id=run.id,
                    task_id=task.id,
                )
                return False
            artifact = create_artifact_record(
                db=db,
                task_id=task.id,
                artifact_type="echo_result",
                content_type="application/json",
                data=json.dumps(task.params_json).encode("utf-8"),
                event_logger=logger,
            )
            result = {
                "artifact_ids": [artifact.id],
                "summary": "echo result",
            }
        elif task.type == "sleep":
            seconds = int(task.params_json.get("seconds", 0))
            elapsed = 0
            while elapsed < seconds:
                task = refresh_task(db, task)
                if task.cancel_requested:
                    mark_cancelled(db, task)
                    update_run_status(db, run, "failed")
                    create_metric(
                        db,
                        name="duration_ms",
                        value=int((time.time() - start_time) * 1000),
                        run_id=run.id,
                        task_id=task.id,
                    )
                    return False
                if time.time() - start_time > TASK_MAX_RUNTIME_SECONDS:
                    log_event(logger, "task_killed", task_id=task.id, reason="runtime_limit", limit=TASK_MAX_RUNTIME_SECONDS)
                    mark_failed(db, task, "task_runtime_exceeded")
                    update_run_status(db, run, "failed")
                    return False
                if time.time() - start_time > task.timeout_seconds:
                    raise TimeoutError("task timed out")
                time.sleep(1)
                elapsed += 1
            artifact = create_artifact_record(
                db=db,
                task_id=task.id,
                artifact_type="sleep_result",
                content_type="application/json",
                data=json.dumps({"slept_seconds": seconds}).encode("utf-8"),
                event_logger=logger,
            )
            result = {
                "artifact_ids": [artifact.id],
                "summary": f"slept {seconds} seconds",
            }
        elif task.type == "search":
            task = refresh_task(db, task)
            if task.cancel_requested:
                mark_cancelled(db, task)
                update_run_status(db, run, "failed")
                create_metric(
                    db,
                    name="duration_ms",
                    value=int((time.time() - start_time) * 1000),
                    run_id=run.id,
                    task_id=task.id,
                )
                return False
            sources = (task.params_json.get("sources") or ["brave"])
            if not isinstance(sources, list):
                log_event(
                    logger,
                    "task_rejected_invalid_params",
                    task_id=task.id,
                    reason="search_sources_invalid",
                    allowlist=SEARCH_SOURCE_ALLOWLIST,
                )
                mark_failed(db, task, "task_rejected_invalid_params")
                update_run_status(db, run, "failed")
                return False
            if "exa" in [source.lower() for source in sources] and not EXA_API_KEY:
                log_event(
                    logger,
                    "task_rejected_invalid_params",
                    task_id=task.id,
                    reason="exa_api_key_missing",
                )
                mark_failed(db, task, "task_rejected_invalid_params")
                update_run_status(db, run, "failed")
                return False
            if any(source.lower() not in SEARCH_SOURCE_ALLOWLIST for source in sources):
                log_event(
                    logger,
                    "task_rejected_invalid_params",
                    task_id=task.id,
                    reason="search_source_not_allowed",
                    allowlist=SEARCH_SOURCE_ALLOWLIST,
                )
                mark_failed(db, task, "task_rejected_invalid_params")
                update_run_status(db, run, "failed")
                return False
            if time.time() - start_time > TASK_MAX_RUNTIME_SECONDS:
                log_event(logger, "task_killed", task_id=task.id, reason="runtime_limit", limit=TASK_MAX_RUNTIME_SECONDS)
                mark_failed(db, task, "task_runtime_exceeded")
                update_run_status(db, run, "failed")
                return False
            redis_client = connect_redis()
            result = run_search_task(db, task.id, task.params_json, redis_client, logger)
            if time.time() - start_time > TASK_MAX_RUNTIME_SECONDS:
                log_event(logger, "task_killed", task_id=task.id, reason="runtime_limit", limit=TASK_MAX_RUNTIME_SECONDS)
                mark_failed(db, task, "task_runtime_exceeded")
                update_run_status(db, run, "failed")
                return False
            task = refresh_task(db, task)
            if task.cancel_requested:
                mark_cancelled(db, task)
                update_run_status(db, run, "failed")
                create_metric(
                    db,
                    name="duration_ms",
                    value=int((time.time() - start_time) * 1000),
                    run_id=run.id,
                    task_id=task.id,
                )
                return False
        elif task.type == "search_batch":
            task = refresh_task(db, task)
            if task.cancel_requested:
                mark_cancelled(db, task)
                update_run_status(db, run, "failed")
                create_metric(
                    db,
                    name="duration_ms",
                    value=int((time.time() - start_time) * 1000),
                    run_id=run.id,
                    task_id=task.id,
                )
                return False
            try:
                normalized = normalize_batch_params(task.params_json)
            except Exception as exc:
                log_event(
                    logger,
                    "task_rejected_invalid_params",
                    task_id=task.id,
                    reason=str(exc),
                )
                mark_failed(db, task, "task_rejected_invalid_params")
                update_run_status(db, run, "failed")
                return False
            sources = normalized["sources"]
            queries = normalized["queries"]
            if "exa" in [source.lower() for source in sources] and not EXA_API_KEY:
                log_event(
                    logger,
                    "task_rejected_invalid_params",
                    task_id=task.id,
                    reason="exa_api_key_missing",
                )
                mark_failed(db, task, "task_rejected_invalid_params")
                update_run_status(db, run, "failed")
                return False
            if len(queries) > MAX_BATCH_SIZE:
                log_event(
                    logger,
                    "task_rejected_invalid_params",
                    task_id=task.id,
                    reason="batch_size_exceeded",
                    limit=MAX_BATCH_SIZE,
                )
                mark_failed(db, task, "task_rejected_invalid_params")
                update_run_status(db, run, "failed")
                return False
            if not search_sources_allowed(sources):
                log_event(
                    logger,
                    "task_rejected_invalid_params",
                    task_id=task.id,
                    reason="search_source_not_allowed",
                    allowlist=SEARCH_SOURCE_ALLOWLIST,
                )
                mark_failed(db, task, "task_rejected_invalid_params")
                update_run_status(db, run, "failed")
                return False
            if time.time() - start_time > TASK_MAX_RUNTIME_SECONDS:
                log_event(logger, "task_killed", task_id=task.id, reason="runtime_limit", limit=TASK_MAX_RUNTIME_SECONDS)
                mark_failed(db, task, "task_runtime_exceeded")
                update_run_status(db, run, "failed")
                return False
            redis_client = connect_redis()

            def should_abort() -> Optional[str]:
                nonlocal task
                task = refresh_task(db, task)
                if task.cancel_requested:
                    return "cancelled"
                if time.time() - start_time > TASK_MAX_RUNTIME_SECONDS:
                    return "runtime_limit"
                return None

            try:
                result = run_search_batch_task(
                    db,
                    task.id,
                    task.params_json,
                    redis_client,
                    logger,
                    normalized=normalized,
                    should_abort=should_abort,
                )
            except TaskCancelled:
                mark_cancelled(db, task)
                update_run_status(db, run, "failed")
                create_metric(
                    db,
                    name="duration_ms",
                    value=int((time.time() - start_time) * 1000),
                    run_id=run.id,
                    task_id=task.id,
                )
                return False
            except TaskRuntimeExceeded:
                log_event(logger, "task_killed", task_id=task.id, reason="runtime_limit", limit=TASK_MAX_RUNTIME_SECONDS)
                mark_failed(db, task, "task_runtime_exceeded")
                update_run_status(db, run, "failed")
                return False
            if time.time() - start_time > TASK_MAX_RUNTIME_SECONDS:
                log_event(logger, "task_killed", task_id=task.id, reason="runtime_limit", limit=TASK_MAX_RUNTIME_SECONDS)
                mark_failed(db, task, "task_runtime_exceeded")
                update_run_status(db, run, "failed")
                return False
            task = refresh_task(db, task)
            if task.cancel_requested:
                mark_cancelled(db, task)
                update_run_status(db, run, "failed")
                create_metric(
                    db,
                    name="duration_ms",
                    value=int((time.time() - start_time) * 1000),
                    run_id=run.id,
                    task_id=task.id,
                )
                return False
        elif task.type == "fetch_url":
            task = refresh_task(db, task)
            if task.cancel_requested:
                mark_cancelled(db, task)
                update_run_status(db, run, "failed")
                create_metric(
                    db,
                    name="duration_ms",
                    value=int((time.time() - start_time) * 1000),
                    run_id=run.id,
                    task_id=task.id,
                )
                return False
            try:
                normalized = normalize_fetch_params(task.params_json)
            except Exception as exc:
                log_event(
                    logger,
                    "task_rejected_invalid_params",
                    task_id=task.id,
                    reason=str(exc),
                )
                mark_failed(db, task, "task_rejected_invalid_params")
                update_run_status(db, run, "failed")
                return False
            canonical_url = normalized["canonical_url"]
            if not validate_fetch_domain(canonical_url):
                log_event(
                    logger,
                    "task_rejected_invalid_params",
                    task_id=task.id,
                    reason="fetch_domain_not_allowed",
                )
                mark_failed(db, task, "task_rejected_invalid_params")
                update_run_status(db, run, "failed")
                return False
            if time.time() - start_time > TASK_MAX_RUNTIME_SECONDS:
                log_event(logger, "task_killed", task_id=task.id, reason="runtime_limit", limit=TASK_MAX_RUNTIME_SECONDS)
                mark_failed(db, task, "task_runtime_exceeded")
                update_run_status(db, run, "failed")
                return False
            redis_client = connect_redis()
            result = run_fetch_task(db, task.id, task.params_json, redis_client, logger)
            if time.time() - start_time > TASK_MAX_RUNTIME_SECONDS:
                log_event(logger, "task_killed", task_id=task.id, reason="runtime_limit", limit=TASK_MAX_RUNTIME_SECONDS)
                mark_failed(db, task, "task_runtime_exceeded")
                update_run_status(db, run, "failed")
                return False
            task = refresh_task(db, task)
            if task.cancel_requested:
                mark_cancelled(db, task)
                update_run_status(db, run, "failed")
                create_metric(
                    db,
                    name="duration_ms",
                    value=int((time.time() - start_time) * 1000),
                    run_id=run.id,
                    task_id=task.id,
                )
                return False
        elif task.type == "import_file":
            task = refresh_task(db, task)
            if task.cancel_requested:
                mark_cancelled(db, task)
                update_run_status(db, run, "failed")
                create_metric(
                    db,
                    name="duration_ms",
                    value=int((time.time() - start_time) * 1000),
                    run_id=run.id,
                    task_id=task.id,
                )
                return False
            result = run_import_file_task(db, task.id, task.params_json, logger)
        elif task.type == "import_chatgpt":
            task = refresh_task(db, task)
            if task.cancel_requested:
                mark_cancelled(db, task)
                update_run_status(db, run, "failed")
                create_metric(
                    db,
                    name="duration_ms",
                    value=int((time.time() - start_time) * 1000),
                    run_id=run.id,
                    task_id=task.id,
                )
                return False
            result = run_import_chatgpt_task(db, task.id, task.params_json, logger)
        elif task.type == "knowledge_promote":
            task = refresh_task(db, task)
            if task.cancel_requested:
                mark_cancelled(db, task)
                update_run_status(db, run, "failed")
                create_metric(
                    db,
                    name="duration_ms",
                    value=int((time.time() - start_time) * 1000),
                    run_id=run.id,
                    task_id=task.id,
                )
                return False
            try:
                result = run_knowledge_promote_task(db, task.id, task.params_json, logger)
            except TaskCancelled:
                mark_cancelled(db, task)
                update_run_status(db, run, "failed")
                create_metric(
                    db,
                    name="duration_ms",
                    value=int((time.time() - start_time) * 1000),
                    run_id=run.id,
                    task_id=task.id,
                )
                return False
            except TaskRuntimeExceeded as exc:
                log_event(logger, "task_killed", task_id=task.id, reason="runtime_limit", detail=str(exc))
                mark_failed(db, task, str(exc))
                update_run_status(db, run, "failed")
                return False
        elif task.type == "knowledge_query":
            task = refresh_task(db, task)
            if task.cancel_requested:
                mark_cancelled(db, task)
                update_run_status(db, run, "failed")
                create_metric(
                    db,
                    name="duration_ms",
                    value=int((time.time() - start_time) * 1000),
                    run_id=run.id,
                    task_id=task.id,
                )
                return False
            result = run_knowledge_query_task(db, task.id, task.params_json, logger)
        else:
            raise ValueError("unsupported task type")

        task.result_json = result
        task.error = None
        task.finished_at = datetime.utcnow()
        transition_status(db, task, "completed")
        update_run_status(db, run, "completed")
        create_metric(
            db,
            name="duration_ms",
            value=int((time.time() - start_time) * 1000),
            run_id=run.id,
            task_id=task.id,
        )
        return False
    except Exception as exc:
        now = datetime.utcnow()
        task.error = str(exc)
        task.updated_at = now
        if task.retry_count <= task.max_retries:
            task.status = "queued"
            task.started_at = None
            task.finished_at = None
            db.commit()
            log_task_transition(
                logger,
                task.id,
                task.type,
                "running",
                "queued",
                retry_count=task.retry_count,
                reason="retry",
                error=str(exc),
            )
            r = connect_redis()
            ensure_enqueued(r, task.id, task.type)
            update_run_status(db, run, "failed")
            create_metric(
                db,
                name="duration_ms",
                value=int((time.time() - start_time) * 1000),
                run_id=run.id,
                task_id=task.id,
            )
            return True
        else:
            update_run_status(db, run, "failed")
            create_metric(
                db,
                name="duration_ms",
                value=int((time.time() - start_time) * 1000),
                run_id=run.id,
                task_id=task.id,
            )
            mark_failed(db, task, str(exc))
            return False


def main() -> None:
    if WORKER_QUEUE not in VALID_WORKER_QUEUES:
        raise ValueError(f"unsupported WORKER_QUEUE: {WORKER_QUEUE}")
    init_db()
    log_event(
        logger,
        "worker_startup",
        queue=WORKER_QUEUE,
        processing_queue=get_processing_name(WORKER_QUEUE),
        queue_set=get_queue_set(WORKER_QUEUE),
    )
    preload_embedding_provider()

    t = threading.Thread(target=heartbeat_loop, daemon=True)
    t.start()

    r = connect_redis()
    try:
        ensure_bucket(get_client())
    except Exception:
        pass

    db = SessionLocal()
    try:
        recover_queue_state(r, db)
    finally:
        db.close()

    last_reconcile = 0.0
    while True:
        try:
            task_id: Optional[str] = dequeue_task(r, POLL_TIMEOUT, WORKER_QUEUE)
        except Exception as exc:
            log_event(logger, "redis_error", error=str(exc))
            r = connect_redis()
            db = SessionLocal()
            try:
                recover_queue_state(r, db)
            finally:
                db.close()
            continue

        if not task_id:
            now = time.time()
            if now - last_reconcile >= RECONCILE_INTERVAL:
                db = SessionLocal()
                try:
                    ensure_queued_tasks(r, db)
                finally:
                    db.close()
                last_reconcile = now
            continue

        log_event(logger, "task_dequeued", task_id=task_id, queue=WORKER_QUEUE)

        db = SessionLocal()
        try:
            task = db.query(Task).filter(Task.id == task_id).first()
            if not task:
                try:
                    ack_task(r, task_id, WORKER_QUEUE)
                except Exception:
                    r = connect_redis()
                    ack_task(r, task_id, WORKER_QUEUE)
                continue
            if not task_belongs_to_worker(task):
                log_event(
                    logger,
                    "task_wrong_queue",
                    task_id=task.id,
                    task_type=task.type,
                    worker_queue=WORKER_QUEUE,
                    expected_queue=get_queue_for_task(task.type),
                )
                try:
                    ack_task(r, task_id, WORKER_QUEUE)
                    ensure_enqueued(r, task.id, task.type)
                except Exception:
                    r = connect_redis()
                    ack_task(r, task_id, WORKER_QUEUE)
                    ensure_enqueued(r, task.id, task.type)
                continue
            if task.status != "queued":
                try:
                    ack_task(r, task_id, WORKER_QUEUE)
                except Exception:
                    r = connect_redis()
                    ack_task(r, task_id, WORKER_QUEUE)
                continue
            requeued = execute_task(db, task)
            try:
                if requeued:
                    ack_processing(r, task_id, WORKER_QUEUE)
                else:
                    ack_task(r, task_id, WORKER_QUEUE)
            except Exception:
                r = connect_redis()
                if requeued:
                    ack_processing(r, task_id, WORKER_QUEUE)
                else:
                    ack_task(r, task_id, WORKER_QUEUE)
        finally:
            db.close()


if __name__ == "__main__":
    main()
