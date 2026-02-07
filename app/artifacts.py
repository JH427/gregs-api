import base64
import uuid
from datetime import datetime
from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import StreamingResponse
from starlette.background import BackgroundTask
from pydantic import BaseModel
from sqlalchemy.orm import Session

from app.db import get_db
from app.logging_utils import get_logger, log_event
from app.limits import TASK_MAX_ARTIFACT_MB
from app.models import Artifact
from app.storage import get_client, get_object, put_bytes

router = APIRouter()
logger = get_logger("api")


class ArtifactCreateRequest(BaseModel):
    task_id: str
    type: str
    content_type: str
    data_base64: str


class ArtifactResponse(BaseModel):
    artifact_id: str
    task_id: str
    type: str
    content_type: str
    created_at: str


def build_object_path(task_id: str, artifact_type: str, artifact_id: str) -> str:
    return f"tasks/{task_id}/{artifact_type}/{artifact_id}"


def create_artifact_record(
    db: Session,
    task_id: str,
    artifact_type: str,
    content_type: str,
    data: bytes,
    event_logger=None,
) -> Artifact:
    size_mb = len(data) / (1024 * 1024)
    if size_mb > TASK_MAX_ARTIFACT_MB:
        log_event(
            event_logger,
            "artifact_rejected",
            task_id=task_id,
            type=artifact_type,
            size_mb=round(size_mb, 2),
            limit_mb=TASK_MAX_ARTIFACT_MB,
            reason="artifact_size_exceeded",
        )
        raise ValueError("artifact_size_exceeded")
    artifact_id = str(uuid.uuid4())
    path = build_object_path(task_id, artifact_type, artifact_id)

    if event_logger is None:
        event_logger = logger

    client = get_client()
    put_bytes(client, path, data, content_type)
    log_event(event_logger, "artifact_stored", artifact_id=artifact_id, task_id=task_id, type=artifact_type)

    artifact = Artifact(
        id=artifact_id,
        task_id=task_id,
        type=artifact_type,
        content_type=content_type,
        path=path,
        created_at=datetime.utcnow(),
    )
    db.add(artifact)
    db.commit()
    log_event(event_logger, "artifact_created", artifact_id=artifact_id, task_id=task_id, type=artifact_type)
    return artifact


@router.post("/artifacts", response_model=ArtifactResponse)
def create_artifact(payload: ArtifactCreateRequest, db: Session = Depends(get_db)) -> ArtifactResponse:
    try:
        data = base64.b64decode(payload.data_base64)
    except Exception:
        raise HTTPException(status_code=400, detail="invalid data_base64")

    artifact = create_artifact_record(
        db=db,
        task_id=payload.task_id,
        artifact_type=payload.type,
        content_type=payload.content_type,
        data=data,
    )

    return ArtifactResponse(
        artifact_id=artifact.id,
        task_id=artifact.task_id,
        type=artifact.type,
        content_type=artifact.content_type,
        created_at=artifact.created_at.isoformat() + "Z",
    )


@router.get("/artifacts/{artifact_id}")
def get_artifact(artifact_id: str, db: Session = Depends(get_db)):
    artifact = db.query(Artifact).filter(Artifact.id == artifact_id).first()
    if not artifact:
        log_event(logger, "artifact_fetch_failed", artifact_id=artifact_id, reason="not_found")
        raise HTTPException(status_code=404, detail="artifact not found")

    log_event(logger, "artifact_fetch_requested", artifact_id=artifact.id, task_id=artifact.task_id, type=artifact.type)

    try:
        client = get_client()
        obj = get_object(client, artifact.path)
    except Exception as exc:
        log_event(
            logger,
            "artifact_fetch_failed",
            artifact_id=artifact.id,
            task_id=artifact.task_id,
            type=artifact.type,
            reason="storage_error",
            error=str(exc),
        )
        raise HTTPException(status_code=500, detail="artifact fetch failed")

    return StreamingResponse(obj, media_type=artifact.content_type, background=BackgroundTask(obj.close))


@router.get("/artifacts/{artifact_id}/meta", response_model=ArtifactResponse)
def get_artifact_metadata(artifact_id: str, db: Session = Depends(get_db)) -> ArtifactResponse:
    artifact = db.query(Artifact).filter(Artifact.id == artifact_id).first()
    if not artifact:
        raise HTTPException(status_code=404, detail="artifact not found")

    log_event(logger, "artifact_fetch_requested", artifact_id=artifact.id, task_id=artifact.task_id, type=artifact.type)

    return ArtifactResponse(
        artifact_id=artifact.id,
        task_id=artifact.task_id,
        type=artifact.type,
        content_type=artifact.content_type,
        created_at=artifact.created_at.isoformat() + "Z",
    )
