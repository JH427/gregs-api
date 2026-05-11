from typing import Optional

from fastapi import APIRouter, Depends, Query, status
from sqlalchemy.orm import Session

from app.board.auth import BoardActor, get_board_actor
from app.board.schemas import (
    BoardAgentHeartbeatRequest,
    BoardAgentListResponse,
    BoardAgentRecord,
    BoardAgentRegisterRequest,
    BoardCommentCreateRequest,
    BoardCommentListResponse,
    BoardCommentRecord,
    BoardEventListResponse,
    BoardTaskBlockRequest,
    BoardTaskCancelRequest,
    BoardTaskClaimRequest,
    BoardTaskCompleteRequest,
    BoardTaskCreateRequest,
    BoardTaskFailRequest,
    BoardTaskHeartbeatRequest,
    BoardTaskListResponse,
    BoardTaskPatchRequest,
    BoardTaskRecord,
    BoardTaskReleaseRequest,
    BoardTaskStartRequest,
    BoardTaskArtifactCreateRequest,
    BoardTaskArtifactListResponse,
    BoardTaskArtifactRecord,
)
from app.board.service import (
    block_board_task,
    cancel_board_task,
    claim_board_task,
    complete_board_task,
    create_board_comment,
    create_board_task,
    create_board_task_artifact,
    fail_board_task,
    get_board_task,
    heartbeat_board_agent,
    heartbeat_board_task,
    list_board_agents,
    list_board_comments,
    list_board_events,
    list_board_task_artifacts,
    list_board_tasks,
    patch_board_task,
    release_board_task,
    register_board_agent,
    start_board_task,
)
from app.db import get_db


router = APIRouter(prefix="/api/board", tags=["board"])


@router.post("/agents/register", response_model=BoardAgentRecord, status_code=status.HTTP_201_CREATED)
def register_agent(
    payload: BoardAgentRegisterRequest,
    db: Session = Depends(get_db),
    actor: BoardActor = Depends(get_board_actor),
) -> BoardAgentRecord:
    return register_board_agent(db, payload, actor)


@router.post("/agents/heartbeat", response_model=BoardAgentRecord)
def heartbeat_agent(
    payload: BoardAgentHeartbeatRequest,
    db: Session = Depends(get_db),
    actor: BoardActor = Depends(get_board_actor),
) -> BoardAgentRecord:
    return heartbeat_board_agent(db, payload, actor)


@router.get("/agents", response_model=BoardAgentListResponse)
def get_agents(
    db: Session = Depends(get_db),
    actor: BoardActor = Depends(get_board_actor),
) -> BoardAgentListResponse:
    return BoardAgentListResponse(agents=list_board_agents(db))


@router.post("/tasks", response_model=BoardTaskRecord, status_code=status.HTTP_201_CREATED)
def create_task(
    payload: BoardTaskCreateRequest,
    db: Session = Depends(get_db),
    actor: BoardActor = Depends(get_board_actor),
) -> BoardTaskRecord:
    return create_board_task(db, payload, actor)


@router.get("/tasks", response_model=BoardTaskListResponse)
def get_tasks(
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    status_filter: Optional[str] = Query(None, alias="status"),
    assignee: Optional[str] = Query(None),
    requested_capability: Optional[str] = Query(None),
    claimed_by: Optional[str] = Query(None),
    parent_task_id: Optional[str] = Query(None),
    db: Session = Depends(get_db),
    actor: BoardActor = Depends(get_board_actor),
) -> BoardTaskListResponse:
    total, tasks = list_board_tasks(
        db,
        limit=limit,
        offset=offset,
        status=status_filter,
        assignee=assignee,
        requested_capability=requested_capability,
        claimed_by=claimed_by,
        parent_task_id=parent_task_id,
    )
    return BoardTaskListResponse(total=total, limit=limit, offset=offset, tasks=tasks)


@router.get("/tasks/{task_id}", response_model=BoardTaskRecord)
def get_task(
    task_id: str,
    db: Session = Depends(get_db),
    actor: BoardActor = Depends(get_board_actor),
) -> BoardTaskRecord:
    return get_board_task(db, task_id)


@router.patch("/tasks/{task_id}", response_model=BoardTaskRecord)
def update_task(
    task_id: str,
    payload: BoardTaskPatchRequest,
    db: Session = Depends(get_db),
    actor: BoardActor = Depends(get_board_actor),
) -> BoardTaskRecord:
    return patch_board_task(db, task_id, payload, actor)


@router.post("/tasks/{task_id}/claim", response_model=BoardTaskRecord)
def claim_task(
    task_id: str,
    payload: BoardTaskClaimRequest,
    db: Session = Depends(get_db),
    actor: BoardActor = Depends(get_board_actor),
) -> BoardTaskRecord:
    return claim_board_task(db, task_id, payload, actor)


@router.post("/tasks/{task_id}/heartbeat", response_model=BoardTaskRecord)
def heartbeat_task(
    task_id: str,
    payload: BoardTaskHeartbeatRequest,
    db: Session = Depends(get_db),
    actor: BoardActor = Depends(get_board_actor),
) -> BoardTaskRecord:
    return heartbeat_board_task(db, task_id, payload, actor)


@router.post("/tasks/{task_id}/release", response_model=BoardTaskRecord)
def release_task(
    task_id: str,
    payload: BoardTaskReleaseRequest,
    db: Session = Depends(get_db),
    actor: BoardActor = Depends(get_board_actor),
) -> BoardTaskRecord:
    return release_board_task(db, task_id, payload, actor)


@router.post("/tasks/{task_id}/start", response_model=BoardTaskRecord)
def start_task(
    task_id: str,
    payload: BoardTaskStartRequest,
    db: Session = Depends(get_db),
    actor: BoardActor = Depends(get_board_actor),
) -> BoardTaskRecord:
    return start_board_task(db, task_id, payload, actor)


@router.post("/tasks/{task_id}/complete", response_model=BoardTaskRecord)
def complete_task(
    task_id: str,
    payload: BoardTaskCompleteRequest,
    db: Session = Depends(get_db),
    actor: BoardActor = Depends(get_board_actor),
) -> BoardTaskRecord:
    return complete_board_task(db, task_id, payload, actor)


@router.post("/tasks/{task_id}/block", response_model=BoardTaskRecord)
def block_task(
    task_id: str,
    payload: BoardTaskBlockRequest,
    db: Session = Depends(get_db),
    actor: BoardActor = Depends(get_board_actor),
) -> BoardTaskRecord:
    return block_board_task(db, task_id, payload, actor)


@router.post("/tasks/{task_id}/fail", response_model=BoardTaskRecord)
def fail_task(
    task_id: str,
    payload: BoardTaskFailRequest,
    db: Session = Depends(get_db),
    actor: BoardActor = Depends(get_board_actor),
) -> BoardTaskRecord:
    return fail_board_task(db, task_id, payload, actor)


@router.post("/tasks/{task_id}/cancel", response_model=BoardTaskRecord)
def cancel_task(
    task_id: str,
    payload: BoardTaskCancelRequest,
    db: Session = Depends(get_db),
    actor: BoardActor = Depends(get_board_actor),
) -> BoardTaskRecord:
    return cancel_board_task(db, task_id, payload, actor)


@router.get("/tasks/{task_id}/comments", response_model=BoardCommentListResponse)
def get_task_comments(
    task_id: str,
    db: Session = Depends(get_db),
    actor: BoardActor = Depends(get_board_actor),
) -> BoardCommentListResponse:
    return BoardCommentListResponse(comments=list_board_comments(db, task_id))


@router.post("/tasks/{task_id}/comments", response_model=BoardCommentRecord, status_code=status.HTTP_201_CREATED)
def create_task_comment(
    task_id: str,
    payload: BoardCommentCreateRequest,
    db: Session = Depends(get_db),
    actor: BoardActor = Depends(get_board_actor),
) -> BoardCommentRecord:
    return create_board_comment(db, task_id, payload, actor)


@router.get("/tasks/{task_id}/artifacts", response_model=BoardTaskArtifactListResponse)
def get_task_artifacts(
    task_id: str,
    db: Session = Depends(get_db),
    actor: BoardActor = Depends(get_board_actor),
) -> BoardTaskArtifactListResponse:
    return BoardTaskArtifactListResponse(artifacts=list_board_task_artifacts(db, task_id))


@router.post("/tasks/{task_id}/artifacts", response_model=BoardTaskArtifactRecord, status_code=status.HTTP_201_CREATED)
def create_task_artifact(
    task_id: str,
    payload: BoardTaskArtifactCreateRequest,
    db: Session = Depends(get_db),
    actor: BoardActor = Depends(get_board_actor),
) -> BoardTaskArtifactRecord:
    return create_board_task_artifact(db, task_id, payload, actor)


@router.get("/events", response_model=BoardEventListResponse)
def get_events(
    task_id: Optional[str] = Query(None),
    limit: int = Query(100, ge=1, le=500),
    db: Session = Depends(get_db),
    actor: BoardActor = Depends(get_board_actor),
) -> BoardEventListResponse:
    return BoardEventListResponse(events=list_board_events(db, task_id=task_id, limit=limit))
