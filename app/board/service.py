import base64
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from fastapi import HTTPException
from sqlalchemy.orm import Session

from app.artifacts import create_artifact_record
from app.board.auth import (
    BoardActor,
    is_worker_actor,
    require_admin_actor,
    require_worker_self,
    worker_allows_capability,
)
from app.board.constants import BOARD_AGENT_STATUSES, BOARD_COMMENT_TYPES, BOARD_TASK_STATUSES
from app.board.events import create_board_event
from app.limits import (
    BOARD_CLAIM_TTL_SECONDS,
    BOARD_MAX_CHILD_TASKS,
    BOARD_MAX_COMMENT_CHARS,
    BOARD_MAX_COMMENTS_PER_TASK,
    BOARD_MAX_REASSIGNMENTS,
    BOARD_MAX_TASK_BODY_CHARS,
)
from app.board.schemas import (
    BoardCommentCreateRequest,
    BoardCommentRecord,
    BoardAgentHeartbeatRequest,
    BoardAgentRecord,
    BoardAgentRegisterRequest,
    BoardEventRecord,
    BoardTaskBlockRequest,
    BoardTaskCancelRequest,
    BoardTaskClaimRequest,
    BoardTaskCompleteRequest,
    BoardTaskCreateRequest,
    BoardTaskFailRequest,
    BoardTaskHeartbeatRequest,
    BoardTaskPatchRequest,
    BoardTaskRecord,
    BoardTaskReleaseRequest,
    BoardTaskStartRequest,
    BoardTaskArtifactCreateRequest,
    BoardTaskArtifactRecord,
)
from app.models import Artifact, BoardAgent, BoardComment, BoardEvent, BoardTask, BoardTaskArtifact


def validate_board_task_status(value: str) -> str:
    if value not in BOARD_TASK_STATUSES:
        raise HTTPException(status_code=400, detail="invalid board task status")
    return value


def validate_board_agent_status(value: str) -> str:
    if value not in BOARD_AGENT_STATUSES:
        raise HTTPException(status_code=400, detail="invalid board agent status")
    return value


def validate_board_comment_type(value: str) -> str:
    if value not in BOARD_COMMENT_TYPES:
        raise HTTPException(status_code=400, detail="invalid board comment type")
    return value


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _normalize_capabilities(values: list[str]) -> list[str]:
    normalized: list[str] = []
    seen: set[str] = set()
    for value in values:
        item = " ".join(value.split()).strip()
        if not item:
            continue
        lowered = item.lower()
        if lowered in seen:
            continue
        seen.add(lowered)
        normalized.append(item)
    return normalized


def _normalize_text_field(value: str, field_name: str) -> str:
    return _normalize_text_field_with_limit(value, field_name, None)


def _normalize_text_field_with_limit(value: str, field_name: str, max_chars: Optional[int]) -> str:
    normalized = value.strip()
    if not normalized:
        raise HTTPException(status_code=400, detail=f"{field_name} is required")
    if max_chars is not None and len(normalized) > max_chars:
        raise HTTPException(status_code=400, detail=f"{field_name} exceeds max length")
    return normalized


def _task_or_404(db: Session, task_id: str) -> BoardTask:
    task = db.query(BoardTask).filter(BoardTask.id == task_id).first()
    if not task:
        raise HTTPException(status_code=404, detail="board task not found")
    return task


def _agent_or_404(db: Session, agent_id: str) -> BoardAgent:
    agent = db.query(BoardAgent).filter(BoardAgent.id == agent_id).first()
    if not agent:
        raise HTTPException(status_code=404, detail="board agent not found")
    return agent


def _locked_task_or_404(db: Session, task_id: str) -> BoardTask:
    task = (
        db.query(BoardTask)
        .filter(BoardTask.id == task_id)
        .with_for_update()
        .first()
    )
    if not task:
        raise HTTPException(status_code=404, detail="board task not found")
    return task


def serialize_board_agent(agent: BoardAgent) -> BoardAgentRecord:
    return BoardAgentRecord(
        id=agent.id,
        name=agent.name,
        host=agent.host,
        capabilities=list(agent.capabilities or []),
        status=agent.status,
        last_heartbeat=agent.last_heartbeat,
        enabled=bool(agent.enabled),
        metadata=dict(agent.metadata_json or {}),
        created_at=agent.created_at,
        updated_at=agent.updated_at,
    )


def serialize_board_task(task: BoardTask) -> BoardTaskRecord:
    return BoardTaskRecord(
        id=task.id,
        title=task.title,
        body=task.body,
        status=task.status,
        priority=task.priority,
        assignee=task.assignee,
        requested_capability=task.requested_capability,
        created_by=task.created_by,
        claimed_by=task.claimed_by,
        claim_expires_at=task.claim_expires_at,
        parent_task_id=task.parent_task_id,
        workspace_type=task.workspace_type,
        workspace_ref=task.workspace_ref,
        max_retries=task.max_retries,
        retry_count=task.retry_count,
        idempotency_key=task.idempotency_key,
        metadata=_public_metadata(task.metadata_json),
        created_at=task.created_at,
        updated_at=task.updated_at,
        completed_at=task.completed_at,
    )


def serialize_board_comment(comment: BoardComment) -> BoardCommentRecord:
    return BoardCommentRecord(
        id=comment.id,
        task_id=comment.task_id,
        author=comment.author,
        comment_type=comment.comment_type,
        body=comment.body,
        created_at=comment.created_at,
    )


def serialize_board_event(event: BoardEvent) -> BoardEventRecord:
    return BoardEventRecord(
        id=event.id,
        task_id=event.task_id,
        event_type=event.event_type,
        actor=event.actor,
        payload=dict(event.payload_json or {}),
        created_at=event.created_at,
    )


def serialize_board_task_artifact(item: BoardTaskArtifact) -> BoardTaskArtifactRecord:
    return BoardTaskArtifactRecord(
        id=item.id,
        task_id=item.task_id,
        artifact_id=item.artifact_id,
        artifact_type=item.artifact_type,
        path=item.path,
        metadata=dict(item.metadata_json or {}),
        created_by=item.created_by,
        created_at=item.created_at,
    )


def _public_metadata(metadata: Optional[dict[str, Any]]) -> dict[str, Any]:
    if not isinstance(metadata, dict):
        return {}
    return {key: value for key, value in metadata.items() if key != "_system"}


def _system_metadata(metadata: Optional[dict[str, Any]]) -> dict[str, Any]:
    if not isinstance(metadata, dict):
        return {}
    system = metadata.get("_system")
    return dict(system) if isinstance(system, dict) else {}


def _set_system_metadata(task: BoardTask, system: dict[str, Any]) -> None:
    metadata = _public_metadata(task.metadata_json)
    metadata["_system"] = system
    task.metadata_json = metadata


def _set_public_metadata(task: BoardTask, metadata: dict[str, Any]) -> None:
    system = _system_metadata(task.metadata_json)
    merged = dict(metadata)
    if system:
        merged["_system"] = system
    task.metadata_json = merged


def _claim_expired(task: BoardTask) -> bool:
    return bool(task.claim_expires_at and task.claim_expires_at <= _utcnow())


def _require_claim_owner(task: BoardTask, agent_name: str) -> None:
    if task.claimed_by != agent_name:
        raise HTTPException(status_code=409, detail="board task is claimed by another agent")


def _require_live_claim_owner(task: BoardTask, agent_name: str) -> None:
    _require_claim_owner(task, agent_name)
    if _claim_expired(task):
        raise HTTPException(status_code=409, detail="board task claim has expired")


def _clear_claim(task: BoardTask, clear_owner: bool = False) -> None:
    task.claim_expires_at = None
    if clear_owner:
        task.claimed_by = None


def _set_claim(task: BoardTask, agent_name: str, ttl_seconds: int) -> None:
    task.claimed_by = agent_name
    task.claim_expires_at = _utcnow() + timedelta(seconds=ttl_seconds)


def _transition_claimed_task(task: BoardTask, to_status: str) -> None:
    task.status = to_status
    task.updated_at = _utcnow()


def _comment_count_for_task(db: Session, task_id: str) -> int:
    return db.query(BoardComment).filter(BoardComment.task_id == task_id).count()


def _artifact_or_404(db: Session, artifact_id: str) -> Artifact:
    artifact = db.query(Artifact).filter(Artifact.id == artifact_id).first()
    if not artifact:
        raise HTTPException(status_code=404, detail="artifact not found")
    return artifact


def _child_task_count(db: Session, parent_task_id: str) -> int:
    return db.query(BoardTask).filter(BoardTask.parent_task_id == parent_task_id).count()


def _auto_block_task(db: Session, task: BoardTask, actor: BoardActor, reason: str, payload: Optional[dict[str, Any]] = None) -> None:
    if task.status != "blocked":
        task.status = "blocked"
        task.updated_at = _utcnow()
        _clear_claim(task, clear_owner=True)
        db.commit()
    create_board_event(
        db,
        event_type="board_task_auto_blocked",
        actor=actor.name,
        task_id=task.id,
        payload={"reason": reason, **(payload or {})},
    )


def register_board_agent(db: Session, payload: BoardAgentRegisterRequest, actor: BoardActor) -> BoardAgentRecord:
    if is_worker_actor(actor):
        require_worker_self(actor, payload.name)
        if actor.host != payload.host:
            raise HTTPException(status_code=403, detail="worker token host mismatch")
    status = validate_board_agent_status(payload.status)
    capabilities = _normalize_capabilities(payload.capabilities)
    if is_worker_actor(actor):
        allowed = {value.lower() for value in actor.allowed_capabilities}
        if not set(value.lower() for value in capabilities).issubset(allowed):
            raise HTTPException(status_code=403, detail="worker token capabilities mismatch")
    now = _utcnow()

    agent = (
        db.query(BoardAgent)
        .filter(BoardAgent.name == payload.name, BoardAgent.host == payload.host)
        .first()
    )
    created = agent is None
    if created:
        agent = BoardAgent(
            id=str(uuid.uuid4()),
            name=payload.name,
            host=payload.host,
            capabilities=capabilities,
            status=status,
            last_heartbeat=now,
            enabled=payload.enabled,
            metadata_json=payload.metadata,
            created_at=now,
            updated_at=now,
        )
        db.add(agent)
    else:
        agent.capabilities = capabilities
        agent.status = status
        agent.last_heartbeat = now
        agent.enabled = payload.enabled
        agent.metadata_json = payload.metadata
        agent.updated_at = now

    db.commit()
    create_board_event(
        db,
        event_type="board_agent_registered" if created else "board_agent_updated",
        actor=actor.name,
        payload={"agent_id": agent.id, "name": agent.name, "host": agent.host, "status": agent.status},
    )
    db.refresh(agent)
    return serialize_board_agent(agent)


def heartbeat_board_agent(db: Session, payload: BoardAgentHeartbeatRequest, actor: BoardActor) -> BoardAgentRecord:
    agent = _agent_or_404(db, payload.agent_id)
    if is_worker_actor(actor):
        require_worker_self(actor, agent.name)
        if actor.host != agent.host:
            raise HTTPException(status_code=403, detail="worker token host mismatch")
    now = _utcnow()
    agent.last_heartbeat = now
    agent.updated_at = now
    if payload.status is not None:
        agent.status = validate_board_agent_status(payload.status)
    if payload.metadata is not None:
        agent.metadata_json = payload.metadata
    db.commit()
    create_board_event(
        db,
        event_type="board_agent_heartbeat",
        actor=actor.name,
        payload={"agent_id": agent.id, "status": agent.status},
    )
    db.refresh(agent)
    return serialize_board_agent(agent)


def list_board_agents(db: Session) -> list[BoardAgentRecord]:
    agents = db.query(BoardAgent).order_by(BoardAgent.updated_at.desc(), BoardAgent.created_at.desc()).all()
    return [serialize_board_agent(agent) for agent in agents]


def create_board_task(db: Session, payload: BoardTaskCreateRequest, actor: BoardActor) -> BoardTaskRecord:
    require_admin_actor(actor)
    if payload.idempotency_key:
        existing = db.query(BoardTask).filter(BoardTask.idempotency_key == payload.idempotency_key).first()
        if existing:
            return serialize_board_task(existing)

    status = validate_board_task_status(payload.status)
    now = _utcnow()
    parent_task_id: Optional[str] = payload.parent_task_id
    if parent_task_id:
        parent_task = _task_or_404(db, parent_task_id)
        child_count = _child_task_count(db, parent_task_id)
        if child_count >= BOARD_MAX_CHILD_TASKS:
            _auto_block_task(
                db,
                parent_task,
                actor,
                "board_max_child_tasks_exceeded",
                {"child_count": child_count, "limit": BOARD_MAX_CHILD_TASKS},
            )
            raise HTTPException(status_code=409, detail="board max child tasks exceeded")

    task = BoardTask(
        id=str(uuid.uuid4()),
        title=_normalize_text_field(payload.title, "title"),
        body=_normalize_text_field_with_limit(payload.body, "body", BOARD_MAX_TASK_BODY_CHARS),
        status=status,
        priority=payload.priority,
        assignee=payload.assignee,
        requested_capability=payload.requested_capability,
        created_by=payload.created_by,
        claimed_by=None,
        claim_expires_at=None,
        parent_task_id=parent_task_id,
        workspace_type=payload.workspace_type,
        workspace_ref=payload.workspace_ref,
        max_retries=payload.max_retries,
        retry_count=0,
        idempotency_key=payload.idempotency_key,
        metadata_json=dict(payload.metadata),
        created_at=now,
        updated_at=now,
        completed_at=now if status == "done" else None,
    )
    db.add(task)
    db.commit()
    create_board_event(
        db,
        event_type="board_task_created",
        actor=actor.name,
        task_id=task.id,
        payload={"status": task.status, "assignee": task.assignee, "requested_capability": task.requested_capability},
    )
    db.refresh(task)
    return serialize_board_task(task)


def get_board_task(db: Session, task_id: str) -> BoardTaskRecord:
    return serialize_board_task(_task_or_404(db, task_id))


def list_board_tasks(
    db: Session,
    limit: int,
    offset: int,
    status: Optional[str] = None,
    assignee: Optional[str] = None,
    requested_capability: Optional[str] = None,
    claimed_by: Optional[str] = None,
    parent_task_id: Optional[str] = None,
) -> tuple[int, list[BoardTaskRecord]]:
    query = db.query(BoardTask)
    if status:
        query = query.filter(BoardTask.status == validate_board_task_status(status))
    if assignee:
        query = query.filter(BoardTask.assignee == assignee)
    if requested_capability:
        query = query.filter(BoardTask.requested_capability == requested_capability)
    if claimed_by:
        query = query.filter(BoardTask.claimed_by == claimed_by)
    if parent_task_id:
        query = query.filter(BoardTask.parent_task_id == parent_task_id)

    total = query.count()
    tasks = (
        query.order_by(BoardTask.priority.asc(), BoardTask.created_at.desc())
        .offset(offset)
        .limit(limit)
        .all()
    )
    return total, [serialize_board_task(task) for task in tasks]


def patch_board_task(db: Session, task_id: str, payload: BoardTaskPatchRequest, actor: BoardActor) -> BoardTaskRecord:
    require_admin_actor(actor)
    task = _task_or_404(db, task_id)
    updates = payload.model_dump(exclude_unset=True)
    if not updates:
        return serialize_board_task(task)

    if "status" in updates:
        task.status = validate_board_task_status(updates["status"])
        if task.status == "done" and task.completed_at is None:
            task.completed_at = _utcnow()
        elif task.status != "done":
            task.completed_at = None
    if "title" in updates:
        task.title = _normalize_text_field(updates["title"], "title")
    if "body" in updates:
        task.body = _normalize_text_field_with_limit(updates["body"], "body", BOARD_MAX_TASK_BODY_CHARS)
    if "priority" in updates:
        task.priority = updates["priority"]
    if "assignee" in updates:
        previous_assignee = task.assignee
        task.assignee = updates["assignee"]
        if previous_assignee and previous_assignee != task.assignee:
            system = _system_metadata(task.metadata_json)
            reassignments = int(system.get("reassignment_count", 0)) + 1
            system["reassignment_count"] = reassignments
            _set_system_metadata(task, system)
            create_board_event(
                db,
                event_type="board_task_reassigned",
                actor=actor.name,
                task_id=task.id,
                payload={
                    "previous_assignee": previous_assignee,
                    "assignee": task.assignee,
                    "reassignment_count": reassignments,
                },
            )
            if reassignments > BOARD_MAX_REASSIGNMENTS:
                db.commit()
                _auto_block_task(
                    db,
                    task,
                    actor,
                    "board_max_reassignments_exceeded",
                    {"reassignment_count": reassignments, "limit": BOARD_MAX_REASSIGNMENTS},
                )
                db.refresh(task)
                return serialize_board_task(task)
    if "requested_capability" in updates:
        task.requested_capability = updates["requested_capability"]
    if "parent_task_id" in updates:
        parent_task_id = updates["parent_task_id"]
        if parent_task_id:
            if parent_task_id == task.id:
                raise HTTPException(status_code=400, detail="board task cannot be its own parent")
            _task_or_404(db, parent_task_id)
        task.parent_task_id = parent_task_id
    if "workspace_type" in updates:
        task.workspace_type = updates["workspace_type"]
    if "workspace_ref" in updates:
        task.workspace_ref = updates["workspace_ref"]
    if "max_retries" in updates:
        task.max_retries = updates["max_retries"]
    if "metadata" in updates:
        _set_public_metadata(task, updates["metadata"])

    task.updated_at = _utcnow()
    db.commit()
    create_board_event(
        db,
        event_type="board_task_updated",
        actor=actor.name,
        task_id=task.id,
        payload={"updated_fields": sorted(updates.keys())},
    )
    db.refresh(task)
    return serialize_board_task(task)


def claim_board_task(db: Session, task_id: str, payload: BoardTaskClaimRequest, actor: BoardActor) -> BoardTaskRecord:
    require_worker_self(actor, payload.agent_name)
    task = _locked_task_or_404(db, task_id)
    claim_expired = _claim_expired(task)
    if task.status != "ready" and not claim_expired:
        raise HTTPException(status_code=409, detail="board task is not claimable")
    if is_worker_actor(actor):
        if task.assignee and task.assignee != actor.name:
            raise HTTPException(status_code=403, detail="worker token may not claim tasks assigned to another agent")
        if not worker_allows_capability(actor, task.requested_capability):
            raise HTTPException(status_code=403, detail="worker token capability mismatch")

    ttl_seconds = min(payload.claim_ttl_seconds, BOARD_CLAIM_TTL_SECONDS)
    _set_claim(task, payload.agent_name, ttl_seconds)
    task.status = "claimed"
    task.updated_at = _utcnow()
    db.commit()
    create_board_event(
        db,
        event_type="board_task_claimed",
        actor=actor.name,
        task_id=task.id,
        payload={
            "agent_name": payload.agent_name,
            "claim_ttl_seconds": ttl_seconds,
            "previous_status": "expired" if claim_expired else "ready",
        },
    )
    db.refresh(task)
    return serialize_board_task(task)


def heartbeat_board_task(db: Session, task_id: str, payload: BoardTaskHeartbeatRequest, actor: BoardActor) -> BoardTaskRecord:
    require_worker_self(actor, payload.agent_name)
    task = _locked_task_or_404(db, task_id)
    _require_live_claim_owner(task, payload.agent_name)
    if task.status not in {"claimed", "running"}:
        raise HTTPException(status_code=409, detail="board task is not active")

    ttl_seconds = min(payload.claim_ttl_seconds, BOARD_CLAIM_TTL_SECONDS)
    _set_claim(task, payload.agent_name, ttl_seconds)
    task.updated_at = _utcnow()
    db.commit()
    create_board_event(
        db,
        event_type="board_task_heartbeat",
        actor=actor.name,
        task_id=task.id,
        payload={"agent_name": payload.agent_name, "claim_ttl_seconds": ttl_seconds},
    )
    db.refresh(task)
    return serialize_board_task(task)


def release_board_task(db: Session, task_id: str, payload: BoardTaskReleaseRequest, actor: BoardActor) -> BoardTaskRecord:
    require_worker_self(actor, payload.agent_name)
    task = _locked_task_or_404(db, task_id)
    _require_live_claim_owner(task, payload.agent_name)
    release_status = validate_board_task_status(payload.status)
    if release_status in {"claimed", "running", "done", "failed", "cancelled"}:
        raise HTTPException(status_code=400, detail="invalid release status")

    _clear_claim(task, clear_owner=True)
    task.status = release_status
    task.updated_at = _utcnow()
    db.commit()
    create_board_event(
        db,
        event_type="board_task_released",
        actor=actor.name,
        task_id=task.id,
        payload={"agent_name": payload.agent_name, "status": release_status},
    )
    db.refresh(task)
    return serialize_board_task(task)


def start_board_task(db: Session, task_id: str, payload: BoardTaskStartRequest, actor: BoardActor) -> BoardTaskRecord:
    require_worker_self(actor, payload.agent_name)
    task = _locked_task_or_404(db, task_id)
    _require_live_claim_owner(task, payload.agent_name)
    if task.status != "claimed":
        raise HTTPException(status_code=409, detail="board task must be claimed before start")

    _transition_claimed_task(task, "running")
    db.commit()
    create_board_event(
        db,
        event_type="board_task_started",
        actor=actor.name,
        task_id=task.id,
        payload={"agent_name": payload.agent_name},
    )
    db.refresh(task)
    return serialize_board_task(task)


def complete_board_task(db: Session, task_id: str, payload: BoardTaskCompleteRequest, actor: BoardActor) -> BoardTaskRecord:
    require_worker_self(actor, payload.agent_name)
    task = _locked_task_or_404(db, task_id)
    _require_live_claim_owner(task, payload.agent_name)
    if task.status not in {"claimed", "running"}:
        raise HTTPException(status_code=409, detail="board task is not active")

    task.status = "done"
    task.completed_at = _utcnow()
    task.updated_at = task.completed_at
    _clear_claim(task)
    if payload.metadata:
        _set_public_metadata(task, payload.metadata)
    db.commit()
    create_board_event(
        db,
        event_type="board_task_completed",
        actor=actor.name,
        task_id=task.id,
        payload={"agent_name": payload.agent_name},
    )
    db.refresh(task)
    return serialize_board_task(task)


def block_board_task(db: Session, task_id: str, payload: BoardTaskBlockRequest, actor: BoardActor) -> BoardTaskRecord:
    require_worker_self(actor, payload.agent_name)
    task = _locked_task_or_404(db, task_id)
    _require_live_claim_owner(task, payload.agent_name)
    if task.status not in {"claimed", "running"}:
        raise HTTPException(status_code=409, detail="board task is not active")

    task.status = "blocked"
    task.updated_at = _utcnow()
    _clear_claim(task)
    if payload.metadata:
        _set_public_metadata(task, payload.metadata)
    db.commit()
    create_board_event(
        db,
        event_type="board_task_blocked",
        actor=actor.name,
        task_id=task.id,
        payload={"agent_name": payload.agent_name, "reason": payload.reason},
    )
    db.refresh(task)
    return serialize_board_task(task)


def fail_board_task(db: Session, task_id: str, payload: BoardTaskFailRequest, actor: BoardActor) -> BoardTaskRecord:
    require_worker_self(actor, payload.agent_name)
    task = _locked_task_or_404(db, task_id)
    _require_live_claim_owner(task, payload.agent_name)
    if task.status not in {"claimed", "running"}:
        raise HTTPException(status_code=409, detail="board task is not active")

    task.status = "failed"
    task.updated_at = _utcnow()
    _clear_claim(task)
    if payload.metadata:
        _set_public_metadata(task, payload.metadata)
    db.commit()
    create_board_event(
        db,
        event_type="board_task_failed",
        actor=actor.name,
        task_id=task.id,
        payload={"agent_name": payload.agent_name, "error": payload.error},
    )
    db.refresh(task)
    return serialize_board_task(task)


def cancel_board_task(db: Session, task_id: str, payload: BoardTaskCancelRequest, actor: BoardActor) -> BoardTaskRecord:
    require_admin_actor(actor)
    task = _locked_task_or_404(db, task_id)
    task.status = "cancelled"
    task.updated_at = _utcnow()
    _clear_claim(task, clear_owner=True)
    if payload.metadata:
        _set_public_metadata(task, payload.metadata)
    db.commit()
    create_board_event(
        db,
        event_type="board_task_cancelled",
        actor=actor.name,
        task_id=task.id,
        payload={"agent_name": payload.agent_name, "reason": payload.reason},
    )
    db.refresh(task)
    return serialize_board_task(task)


def list_board_comments(db: Session, task_id: str) -> list[BoardCommentRecord]:
    _task_or_404(db, task_id)
    comments = (
        db.query(BoardComment)
        .filter(BoardComment.task_id == task_id)
        .order_by(BoardComment.created_at.asc())
        .all()
    )
    return [serialize_board_comment(comment) for comment in comments]


def create_board_comment(
    db: Session,
    task_id: str,
    payload: BoardCommentCreateRequest,
    actor: BoardActor,
) -> BoardCommentRecord:
    task = _task_or_404(db, task_id)
    if is_worker_actor(actor):
        require_worker_self(actor, payload.author)
        _require_claim_owner(task, actor.name)
    comment_count = _comment_count_for_task(db, task_id)
    if comment_count >= BOARD_MAX_COMMENTS_PER_TASK:
        task = _task_or_404(db, task_id)
        _auto_block_task(
            db,
            task,
            actor,
            "board_max_comments_per_task_exceeded",
            {"comment_count": comment_count, "limit": BOARD_MAX_COMMENTS_PER_TASK},
        )
        raise HTTPException(status_code=409, detail="board max comments per task exceeded")

    comment = BoardComment(
        id=str(uuid.uuid4()),
        task_id=task_id,
        author=_normalize_text_field(payload.author, "author"),
        comment_type=validate_board_comment_type(payload.comment_type),
        body=_normalize_text_field_with_limit(payload.body, "body", BOARD_MAX_COMMENT_CHARS),
        created_at=_utcnow(),
    )
    db.add(comment)
    db.commit()
    create_board_event(
        db,
        event_type="board_comment_created",
        actor=actor.name,
        task_id=task_id,
        payload={
            "comment_id": comment.id,
            "author": comment.author,
            "comment_type": comment.comment_type,
            "comment_count": _comment_count_for_task(db, task_id),
        },
    )
    db.refresh(comment)
    return serialize_board_comment(comment)


def list_board_events(db: Session, task_id: Optional[str] = None, limit: int = 100) -> list[BoardEventRecord]:
    query = db.query(BoardEvent)
    if task_id:
        _task_or_404(db, task_id)
        query = query.filter(BoardEvent.task_id == task_id)
    events = query.order_by(BoardEvent.created_at.desc()).limit(limit).all()
    return [serialize_board_event(event) for event in events]


def list_board_task_artifacts(db: Session, task_id: str) -> list[BoardTaskArtifactRecord]:
    _task_or_404(db, task_id)
    items = (
        db.query(BoardTaskArtifact)
        .filter(BoardTaskArtifact.task_id == task_id)
        .order_by(BoardTaskArtifact.created_at.asc())
        .all()
    )
    return [serialize_board_task_artifact(item) for item in items]


def create_board_task_artifact(
    db: Session,
    task_id: str,
    payload: BoardTaskArtifactCreateRequest,
    actor: BoardActor,
) -> BoardTaskArtifactRecord:
    task = _task_or_404(db, task_id)
    if is_worker_actor(actor):
        require_worker_self(actor, payload.created_by)
        _require_claim_owner(task, actor.name)
    artifact_id = payload.artifact_id
    path = payload.path
    artifact_type = payload.artifact_type

    if payload.data_base64 is not None:
        if not payload.content_type:
            raise HTTPException(status_code=400, detail="content_type is required when uploading artifact content")
        if not artifact_type:
            raise HTTPException(status_code=400, detail="artifact_type is required when uploading artifact content")
        try:
            data = base64.b64decode(payload.data_base64)
        except Exception:
            raise HTTPException(status_code=400, detail="invalid data_base64")
        artifact = create_artifact_record(
            db=db,
            task_id=task_id,
            artifact_type=artifact_type,
            content_type=payload.content_type,
            data=data,
            metadata=payload.metadata,
        )
        artifact_id = artifact.id
        path = artifact.path
        artifact_type = artifact.type
    elif artifact_id:
        artifact = _artifact_or_404(db, artifact_id)
        path = artifact.path
        artifact_type = artifact_type or artifact.type
    elif not path:
        raise HTTPException(status_code=400, detail="artifact_id, path, or uploaded content is required")

    item = BoardTaskArtifact(
        id=str(uuid.uuid4()),
        task_id=task_id,
        artifact_id=artifact_id,
        artifact_type=artifact_type,
        path=path,
        metadata_json=payload.metadata,
        created_by=_normalize_text_field(payload.created_by, "created_by"),
        created_at=_utcnow(),
    )
    db.add(item)
    db.commit()
    create_board_event(
        db,
        event_type="board_task_artifact_created",
        actor=actor.name,
        task_id=task_id,
        payload={
            "board_task_artifact_id": item.id,
            "artifact_id": item.artifact_id,
            "artifact_type": item.artifact_type,
            "created_by": item.created_by,
        },
    )
    db.refresh(item)
    return serialize_board_task_artifact(item)
