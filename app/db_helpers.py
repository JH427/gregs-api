import uuid
from datetime import datetime
from typing import Optional, Tuple

from sqlalchemy.orm import Session

from app.logging_utils import get_logger, log_event
from app.models import Metric, Note, Run

logger = get_logger("db")


def _scope_key(key: str, scope: Optional[str]) -> str:
    return f"{scope}:{key}" if scope else key


def latest_run_for_key(db: Session, run_key: str) -> Optional[Run]:
    return (
        db.query(Run)
        .filter(Run.run_key == run_key)
        .order_by(Run.started_at.desc())
        .first()
    )


def has_been_processed(db: Session, key: str, scope: Optional[str] = None) -> Tuple[bool, Optional[datetime]]:
    run_key = _scope_key(key, scope)
    run = latest_run_for_key(db, run_key)
    if not run or run.status != "completed" or not run.finished_at:
        return False, None
    return True, run.finished_at


def create_run(
    db: Session,
    run_key: str,
    status: str = "running",
    metadata_json: Optional[dict] = None,
) -> Run:
    run = Run(
        id=str(uuid.uuid4()),
        run_key=run_key,
        status=status,
        started_at=datetime.utcnow(),
        finished_at=None,
        metadata_json=metadata_json,
    )
    db.add(run)
    db.commit()
    log_event(logger, "run_created", run_id=run.id, run_key=run.run_key, status=run.status)
    return run


def update_run_status(db: Session, run: Run, status: str) -> None:
    run.status = status
    if status in {"completed", "failed"}:
        run.finished_at = datetime.utcnow()
    db.commit()
    log_event(logger, "run_updated", run_id=run.id, run_key=run.run_key, status=run.status)


def create_metric(
    db: Session,
    name: str,
    value,
    run_id: Optional[str] = None,
    task_id: Optional[str] = None,
) -> Metric:
    metric = Metric(
        id=str(uuid.uuid4()),
        run_id=run_id,
        task_id=task_id,
        name=name,
        value=value,
        created_at=datetime.utcnow(),
    )
    db.add(metric)
    db.commit()
    log_event(
        logger,
        "metric_created",
        metric_id=metric.id,
        name=metric.name,
        run_id=metric.run_id,
        task_id=metric.task_id,
    )
    return metric


def create_note(
    db: Session,
    note_type: str,
    content: str,
    run_id: Optional[str] = None,
    task_id: Optional[str] = None,
) -> Note:
    note = Note(
        id=str(uuid.uuid4()),
        run_id=run_id,
        task_id=task_id,
        type=note_type,
        content=content,
        created_at=datetime.utcnow(),
    )
    db.add(note)
    db.commit()
    log_event(
        logger,
        "note_created",
        note_id=note.id,
        type=note.type,
        run_id=note.run_id,
        task_id=note.task_id,
    )
    return note
