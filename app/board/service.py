import base64
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from fastapi import HTTPException
from sqlalchemy.orm import Session

from app.artifacts import create_artifact_record
from app.board.auth import (
    BoardActor,
    is_admin_actor,
    is_worker_actor,
    require_admin_actor,
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
    BoardAgentPatchRequest,
    BoardAgentRecord,
    BoardAgentRegisterRequest,
    BoardEventRecord,
    BoardTaskBlockRequest,
    BoardTaskCancelRequest,
    BoardTaskClaimRequest,
    BoardTaskCompleteRequest,
    BoardTaskCreateRequest,
    BoardTaskDeleteResponse,
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
        allowed_capabilities=list(task.allowed_capabilities or []),
        watchers=list(task.watchers or []),
        contributors=list(task.contributors or []),
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
        metadata=dict(comment.metadata_json or {}),
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


def _normalize_identity_list(values: Optional[list[str]]) -> list[str]:
    normalized: list[str] = []
    seen: set[str] = set()
    for value in values or []:
        item = " ".join(str(value).split()).strip()
        if not item:
            continue
        lowered = item.lower()
        if lowered in seen:
            continue
        seen.add(lowered)
        normalized.append(item)
    return normalized


def _normalize_identity_value(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    item = " ".join(str(value).split()).strip()
    return item or None


def _identity_matches(lhs: Optional[str], rhs: Optional[str]) -> bool:
    return bool(lhs and rhs and lhs.lower() == rhs.lower())


def _identity_in_list(name: Optional[str], values: Optional[list[str]]) -> bool:
    if not name:
        return False
    lowered = name.lower()
    return any(str(value).strip().lower() == lowered for value in values or [])


def _actor_capability_set(actor: BoardActor) -> set[str]:
    return {value.lower() for value in actor.allowed_capabilities}


def _actor_has_capability(actor: BoardActor, capability: str) -> bool:
    return capability.lower() in _actor_capability_set(actor)


def _task_allowed_capability_set(task: BoardTask) -> set[str]:
    return {str(value).strip().lower() for value in task.allowed_capabilities or [] if str(value).strip()}


def _task_requested_capability_matches(actor: BoardActor, task: BoardTask) -> bool:
    requested = _normalize_identity_value(task.requested_capability)
    if not requested:
        return False
    return requested.lower() in _actor_capability_set(actor)


def _task_allowed_capabilities_match(actor: BoardActor, task: BoardTask) -> bool:
    allowed = _task_allowed_capability_set(task)
    if not allowed:
        return True
    return bool(allowed & _actor_capability_set(actor))


def _is_visible_worker_task(actor: BoardActor, task: BoardTask) -> bool:
    return any(
        [
            _identity_matches(task.assignee, actor.name),
            _identity_matches(task.claimed_by, actor.name),
            _identity_matches(task.created_by, actor.name),
            _identity_in_list(actor.name, task.watchers),
            _identity_in_list(actor.name, task.contributors),
            _task_requested_capability_matches(actor, task),
            _task_allowed_capabilities_match(actor, task),
        ]
    )


def _denial_detail(
    actor: BoardActor,
    task: Optional[BoardTask],
    required_permission: str,
    denial_reason: str,
) -> dict[str, Any]:
    return {
        "actor": actor.name,
        "token_type": actor.token_type,
        "task_id": task.id if task else None,
        "status": task.status if task else None,
        "assignee": task.assignee if task else None,
        "claimed_by": task.claimed_by if task else None,
        "claim_expires_at": task.claim_expires_at.isoformat() if task and task.claim_expires_at else None,
        "required_permission": required_permission,
        "denial_reason": denial_reason,
    }


def _deny(
    db: Session,
    actor: BoardActor,
    required_permission: str,
    denial_reason: str,
    task: Optional[BoardTask] = None,
    status_code: int = 403,
) -> None:
    payload = _denial_detail(actor, task, required_permission, denial_reason)
    create_board_event(
        db,
        event_type="board_authorization_denied",
        actor=actor.name,
        task_id=task.id if task else None,
        payload=payload,
    )
    raise HTTPException(status_code=status_code, detail=payload)


def _ensure_worker_self(
    db: Session,
    actor: BoardActor,
    agent_name: str,
    required_permission: str,
    task: Optional[BoardTask] = None,
) -> None:
    if is_admin_actor(actor):
        return
    if not _identity_matches(actor.name, agent_name):
        _deny(db, actor, required_permission, "worker token may only act as itself", task)


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


def can_view_task(actor: BoardActor, task: BoardTask) -> bool:
    if is_admin_actor(actor):
        return True
    if task.status == "cancelled":
        return _identity_matches(task.assignee, actor.name) or _identity_in_list(actor.name, task.watchers) or _identity_in_list(actor.name, task.contributors)
    return True


def can_comment_task(actor: BoardActor, task: BoardTask) -> bool:
    if task.status == "cancelled":
        return False
    return can_view_task(actor, task)


def can_create_child_task(actor: BoardActor, parent_task: BoardTask) -> bool:
    if is_admin_actor(actor):
        return True
    if parent_task.status == "cancelled":
        return False
    return any(
        [
            _identity_matches(parent_task.assignee, actor.name),
            _identity_matches(parent_task.claimed_by, actor.name),
            _identity_in_list(actor.name, parent_task.watchers),
            _identity_in_list(actor.name, parent_task.contributors),
            _identity_matches(parent_task.created_by, actor.name),
        ]
    )


def can_claim_task(actor: BoardActor, task: BoardTask) -> bool:
    if is_admin_actor(actor):
        return True
    if task.assignee and not _identity_matches(task.assignee, actor.name):
        return False
    if not _task_allowed_capabilities_match(actor, task):
        return False
    if task.requested_capability and not _task_requested_capability_matches(actor, task):
        return False
    if task.status == "ready":
        return True
    return _claim_expired(task)


def can_start_task(actor: BoardActor, task: BoardTask) -> bool:
    if is_admin_actor(actor):
        return True
    return _identity_matches(task.claimed_by, actor.name) and not _claim_expired(task)


def can_complete_task(actor: BoardActor, task: BoardTask) -> bool:
    return can_start_task(actor, task)


def can_transition_task(actor: BoardActor, task: BoardTask, from_status: str, to_status: str) -> bool:
    if is_admin_actor(actor):
        return True
    if task.status != from_status:
        return False
    if to_status == "ready":
        return (
            _identity_matches(task.created_by, actor.name)
            and task.claimed_by is None
            and from_status in {"triage", "todo"}
        )
    return False


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
        _ensure_worker_self(db, actor, payload.name, "register_agent")
        if actor.host != payload.host:
            _deny(db, actor, "register_agent", "worker token host mismatch")
    status = validate_board_agent_status(payload.status)
    capabilities = _normalize_capabilities(payload.capabilities)
    if is_worker_actor(actor):
        allowed = {value.lower() for value in actor.allowed_capabilities}
        if not set(value.lower() for value in capabilities).issubset(allowed):
            _deny(db, actor, "register_agent", "worker token capabilities mismatch")
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
        _ensure_worker_self(db, actor, agent.name, "heartbeat_agent")
        if actor.host != agent.host:
            _deny(db, actor, "heartbeat_agent", "worker token host mismatch")
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


def patch_board_agent(db: Session, agent_id: str, payload: BoardAgentPatchRequest, actor: BoardActor) -> BoardAgentRecord:
    agent = _agent_or_404(db, agent_id)
    updates = payload.model_dump(exclude_unset=True)
    if not updates:
        return serialize_board_agent(agent)
    if is_worker_actor(actor):
        _ensure_worker_self(db, actor, agent.name, "patch_agent")
        if actor.host != agent.host:
            _deny(db, actor, "patch_agent", "worker token host mismatch")
        if "capabilities" in updates:
            normalized_caps = _normalize_capabilities(updates["capabilities"] or [])
            if not set(value.lower() for value in normalized_caps).issubset(_actor_capability_set(actor)):
                _deny(db, actor, "patch_agent", "worker token capabilities mismatch")
            agent.capabilities = normalized_caps
        if "enabled" in updates:
            _deny(db, actor, "patch_agent", "worker token may not toggle enabled")
    else:
        if "capabilities" in updates:
            agent.capabilities = _normalize_capabilities(updates["capabilities"] or [])
    if "status" in updates and updates["status"] is not None:
        agent.status = validate_board_agent_status(updates["status"])
    if "enabled" in updates and is_admin_actor(actor):
        agent.enabled = bool(updates["enabled"])
    if "metadata" in updates and updates["metadata"] is not None:
        agent.metadata_json = updates["metadata"]
    agent.updated_at = _utcnow()
    db.commit()
    create_board_event(
        db,
        event_type="board_agent_updated",
        actor=actor.name,
        payload={"agent_id": agent.id, "name": agent.name, "host": agent.host, "status": agent.status},
    )
    db.refresh(agent)
    return serialize_board_agent(agent)


def list_board_agents(db: Session) -> list[BoardAgentRecord]:
    agents = db.query(BoardAgent).order_by(BoardAgent.updated_at.desc(), BoardAgent.created_at.desc()).all()
    return [serialize_board_agent(agent) for agent in agents]


def create_board_task(db: Session, payload: BoardTaskCreateRequest, actor: BoardActor) -> BoardTaskRecord:
    worker_create = is_worker_actor(actor)
    if not worker_create:
        require_admin_actor(actor)
    if payload.idempotency_key:
        existing = db.query(BoardTask).filter(BoardTask.idempotency_key == payload.idempotency_key).first()
        if existing:
            return serialize_board_task(existing)

    requested_capability = _normalize_identity_value(payload.requested_capability)
    assignee = _normalize_identity_value(payload.assignee)
    allowed_capabilities = _normalize_capabilities(payload.allowed_capabilities)
    watchers = _normalize_identity_list(payload.watchers)
    contributors = _normalize_identity_list(payload.contributors)
    status = validate_board_task_status(payload.status)
    now = _utcnow()
    parent_task_id: Optional[str] = payload.parent_task_id
    parent_task: Optional[BoardTask] = None
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
    elif worker_create:
        _deny(db, actor, "create_child_task", "worker token may only create child tasks")

    if worker_create:
        assert parent_task is not None
        if not can_create_child_task(actor, parent_task):
            _deny(db, actor, "create_child_task", "actor may not create child tasks on this parent", parent_task)
        if (
            not _actor_has_capability(actor, "board_manage_limited")
            and requested_capability
            and not worker_allows_capability(actor, requested_capability)
        ):
            _deny(db, actor, "create_child_task", "worker token cannot request that capability", parent_task)
        if (
            not _actor_has_capability(actor, "board_manage_limited")
            and allowed_capabilities
            and not set(value.lower() for value in allowed_capabilities).issubset(_actor_capability_set(actor))
        ):
            _deny(db, actor, "create_child_task", "worker token cannot grant unowned allowed capabilities", parent_task)
        if not watchers:
            watchers = _normalize_identity_list(list(parent_task.watchers or []))
        if not contributors:
            contributors = _normalize_identity_list(list(parent_task.contributors or []))
        if actor.name not in watchers:
            watchers.append(actor.name)
        if parent_task.assignee and parent_task.assignee not in watchers:
            watchers.append(parent_task.assignee)
        status = "ready" if (assignee or requested_capability) else "triage"
    elif status == "triage" and (assignee or requested_capability):
        status = "ready"

    task = BoardTask(
        id=str(uuid.uuid4()),
        title=_normalize_text_field(payload.title, "title"),
        body=_normalize_text_field_with_limit(payload.body, "body", BOARD_MAX_TASK_BODY_CHARS),
        status=status,
        priority=payload.priority,
        assignee=assignee,
        requested_capability=requested_capability,
        allowed_capabilities=allowed_capabilities,
        watchers=watchers,
        contributors=contributors,
        created_by=actor.name if worker_create else payload.created_by,
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
        payload={
            "status": task.status,
            "assignee": task.assignee,
            "requested_capability": task.requested_capability,
            "allowed_capabilities": list(task.allowed_capabilities or []),
            "watchers": list(task.watchers or []),
            "contributors": list(task.contributors or []),
            "parent_task_id": task.parent_task_id,
            "created_by": task.created_by,
        },
    )
    db.refresh(task)
    return serialize_board_task(task)


def get_board_task(db: Session, task_id: str, actor: BoardActor) -> BoardTaskRecord:
    task = _task_or_404(db, task_id)
    if not can_view_task(actor, task):
        _deny(db, actor, "view_task", "task is not visible to actor", task)
    return serialize_board_task(task)


def list_board_tasks(
    db: Session,
    actor: BoardActor,
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

    tasks = query.order_by(BoardTask.priority.asc(), BoardTask.created_at.desc()).all()
    visible_tasks = [task for task in tasks if can_view_task(actor, task)]
    total = len(visible_tasks)
    window = visible_tasks[offset : offset + limit]
    return total, [serialize_board_task(task) for task in window]


def patch_board_task(db: Session, task_id: str, payload: BoardTaskPatchRequest, actor: BoardActor) -> BoardTaskRecord:
    task = _task_or_404(db, task_id)
    if not can_view_task(actor, task):
        _deny(db, actor, "patch_task", "task is not visible to actor", task)
    updates = payload.model_dump(exclude_unset=True)
    if not updates:
        return serialize_board_task(task)
    if is_worker_actor(actor):
        allowed_status_only = set(updates.keys()).issubset({"status", "metadata"})
        if not allowed_status_only:
            _deny(db, actor, "transition_task", "worker token may only transition status or update metadata", task)
        next_status = updates.get("status")
        if next_status is None or not can_transition_task(actor, task, task.status, next_status):
            _deny(db, actor, "transition_task", "worker token may not perform that status transition", task)

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
        task.assignee = _normalize_identity_value(updates["assignee"])
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
        task.requested_capability = _normalize_identity_value(updates["requested_capability"])
    if "allowed_capabilities" in updates:
        task.allowed_capabilities = _normalize_capabilities(updates["allowed_capabilities"] or [])
    if "watchers" in updates:
        task.watchers = _normalize_identity_list(updates["watchers"] or [])
    if "contributors" in updates:
        task.contributors = _normalize_identity_list(updates["contributors"] or [])
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
    if task.status == "triage" and (task.assignee or task.requested_capability):
        task.status = "ready"

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
    _ensure_worker_self(db, actor, payload.agent_name, "claim_task")
    task = _locked_task_or_404(db, task_id)
    claim_expired = _claim_expired(task)
    if not can_view_task(actor, task):
        _deny(db, actor, "claim_task", "task is not visible to actor", task)
    if task.status != "ready" and not claim_expired:
        _deny(db, actor, "claim_task", "board task is not claimable", task, status_code=409)
    if not can_claim_task(actor, task):
        _deny(db, actor, "claim_task", "actor may not claim this task", task)

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
    _ensure_worker_self(db, actor, payload.agent_name, "heartbeat_task")
    task = _locked_task_or_404(db, task_id)
    if not can_start_task(actor, task):
        _deny(db, actor, "heartbeat_task", "actor does not hold a live claim", task, status_code=409)
    if task.status not in {"claimed", "running"}:
        _deny(db, actor, "heartbeat_task", "board task is not active", task, status_code=409)

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
    _ensure_worker_self(db, actor, payload.agent_name, "release_task")
    task = _locked_task_or_404(db, task_id)
    if not _identity_matches(task.claimed_by, actor.name):
        _deny(db, actor, "release_task", "actor does not own this claim", task, status_code=409)
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
    _ensure_worker_self(db, actor, payload.agent_name, "start_task")
    task = _locked_task_or_404(db, task_id)
    if not can_start_task(actor, task):
        _deny(db, actor, "start_task", "actor does not hold a live claim", task, status_code=409)
    if task.status != "claimed":
        _deny(db, actor, "start_task", "board task must be claimed before start", task, status_code=409)

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
    _ensure_worker_self(db, actor, payload.agent_name, "complete_task")
    task = _locked_task_or_404(db, task_id)
    if not can_complete_task(actor, task):
        _deny(db, actor, "complete_task", "actor does not hold a live claim", task, status_code=409)
    if task.status not in {"claimed", "running"}:
        _deny(db, actor, "complete_task", "board task is not active", task, status_code=409)

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
    _ensure_worker_self(db, actor, payload.agent_name, "block_task")
    task = _locked_task_or_404(db, task_id)
    if not can_complete_task(actor, task):
        _deny(db, actor, "block_task", "actor does not hold a live claim", task, status_code=409)
    if task.status not in {"claimed", "running"}:
        _deny(db, actor, "block_task", "board task is not active", task, status_code=409)

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
    _ensure_worker_self(db, actor, payload.agent_name, "fail_task")
    task = _locked_task_or_404(db, task_id)
    if not can_complete_task(actor, task):
        _deny(db, actor, "fail_task", "actor does not hold a live claim", task, status_code=409)
    if task.status not in {"claimed", "running"}:
        _deny(db, actor, "fail_task", "board task is not active", task, status_code=409)

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
    task = _locked_task_or_404(db, task_id)
    if is_worker_actor(actor):
        _deny(db, actor, "cancel_task", "worker token may not cancel tasks", task)
    require_admin_actor(actor)
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


def _collect_board_task_subtree_ids(db: Session, root_task_id: str) -> list[str]:
    ordered: list[str] = []
    stack = [root_task_id]
    while stack:
        current = stack.pop()
        ordered.append(current)
        children = (
            db.query(BoardTask.id)
            .filter(BoardTask.parent_task_id == current)
            .order_by(BoardTask.created_at.desc())
            .all()
        )
        stack.extend(child_id for (child_id,) in children)
    return ordered


def delete_board_task(db: Session, task_id: str, actor: BoardActor) -> BoardTaskDeleteResponse:
    require_admin_actor(actor)
    _task_or_404(db, task_id)
    task_ids = _collect_board_task_subtree_ids(db, task_id)

    task_events = db.query(BoardEvent).filter(BoardEvent.task_id.in_(task_ids)).all()
    for event in task_events:
        payload = dict(event.payload_json or {})
        payload["deleted_task_id"] = event.task_id
        event.payload_json = payload
        event.task_id = None
    db.flush()

    db.query(BoardComment).filter(BoardComment.task_id.in_(task_ids)).delete(synchronize_session=False)
    db.query(BoardTaskArtifact).filter(BoardTaskArtifact.task_id.in_(task_ids)).delete(synchronize_session=False)
    db.query(BoardTask).filter(BoardTask.id.in_(task_ids)).delete(synchronize_session=False)
    db.commit()

    create_board_event(
        db,
        event_type="board_task_deleted",
        actor=actor.name,
        payload={"root_task_id": task_id, "deleted_task_ids": task_ids, "deleted_count": len(task_ids)},
    )
    return BoardTaskDeleteResponse(deleted_count=len(task_ids), deleted_task_ids=task_ids)


def list_board_comments(db: Session, task_id: str, actor: BoardActor) -> list[BoardCommentRecord]:
    task = _task_or_404(db, task_id)
    if not can_view_task(actor, task):
        _deny(db, actor, "view_task_comments", "task is not visible to actor", task)
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
        _ensure_worker_self(db, actor, payload.author, "comment_task", task)
    if not can_comment_task(actor, task):
        _deny(db, actor, "comment_task", "actor may not comment on this task", task)
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
        metadata_json={
            "token_identity": actor.token_id or actor.name,
            "token_type": actor.token_type,
            "claimed_by_at_comment": task.claimed_by,
            "task_status_at_comment": task.status,
        },
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


def list_board_events(db: Session, actor: BoardActor, task_id: Optional[str] = None, limit: int = 100) -> list[BoardEventRecord]:
    query = db.query(BoardEvent)
    if task_id:
        task = _task_or_404(db, task_id)
        if not can_view_task(actor, task):
            _deny(db, actor, "view_task_events", "task is not visible to actor", task)
        query = query.filter(BoardEvent.task_id == task_id)
    events = query.order_by(BoardEvent.created_at.desc()).limit(limit).all()
    if is_worker_actor(actor) and not task_id:
        visible_ids = {task.id for task in db.query(BoardTask).all() if can_view_task(actor, task)}
        events = [event for event in events if event.task_id is None or event.task_id in visible_ids]
    return [serialize_board_event(event) for event in events]


def list_board_task_artifacts(db: Session, task_id: str, actor: BoardActor) -> list[BoardTaskArtifactRecord]:
    task = _task_or_404(db, task_id)
    if not can_view_task(actor, task):
        _deny(db, actor, "view_task_artifacts", "task is not visible to actor", task)
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
        _ensure_worker_self(db, actor, payload.created_by, "attach_artifact", task)
    if not can_comment_task(actor, task):
        _deny(db, actor, "attach_artifact", "actor may not attach artifacts to this task", task)
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
