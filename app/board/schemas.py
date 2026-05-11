from datetime import datetime
from typing import Any, Optional

from pydantic import BaseModel, Field


class BoardAgentRecord(BaseModel):
    id: str
    name: str
    host: str
    capabilities: list[str] = Field(default_factory=list)
    status: str
    last_heartbeat: Optional[datetime] = None
    enabled: bool
    metadata: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime
    updated_at: datetime


class BoardAgentRegisterRequest(BaseModel):
    name: str
    host: str
    capabilities: list[str] = Field(default_factory=list)
    status: str = "idle"
    enabled: bool = True
    metadata: dict[str, Any] = Field(default_factory=dict)


class BoardAgentHeartbeatRequest(BaseModel):
    agent_id: str
    status: Optional[str] = None
    metadata: Optional[dict[str, Any]] = None


class BoardAgentListResponse(BaseModel):
    agents: list[BoardAgentRecord]


class BoardTaskRecord(BaseModel):
    id: str
    title: str
    body: str
    status: str
    priority: int
    assignee: Optional[str] = None
    requested_capability: Optional[str] = None
    created_by: str
    claimed_by: Optional[str] = None
    claim_expires_at: Optional[datetime] = None
    parent_task_id: Optional[str] = None
    workspace_type: Optional[str] = None
    workspace_ref: Optional[str] = None
    max_retries: int
    retry_count: int
    idempotency_key: Optional[str] = None
    metadata: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime
    updated_at: datetime
    completed_at: Optional[datetime] = None


class BoardTaskCreateRequest(BaseModel):
    title: str
    body: str
    status: str = "triage"
    priority: int = Field(default=3, ge=0)
    assignee: Optional[str] = None
    requested_capability: Optional[str] = None
    created_by: str = "user"
    parent_task_id: Optional[str] = None
    workspace_type: Optional[str] = "scratch"
    workspace_ref: Optional[str] = None
    max_retries: int = Field(default=2, ge=0)
    idempotency_key: Optional[str] = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class BoardTaskPatchRequest(BaseModel):
    title: Optional[str] = None
    body: Optional[str] = None
    status: Optional[str] = None
    priority: Optional[int] = Field(default=None, ge=0)
    assignee: Optional[str] = None
    requested_capability: Optional[str] = None
    parent_task_id: Optional[str] = None
    workspace_type: Optional[str] = None
    workspace_ref: Optional[str] = None
    max_retries: Optional[int] = Field(default=None, ge=0)
    metadata: Optional[dict[str, Any]] = None


class BoardTaskListResponse(BaseModel):
    total: int
    limit: int
    offset: int
    tasks: list[BoardTaskRecord]


class BoardTaskClaimRequest(BaseModel):
    agent_name: str
    claim_ttl_seconds: int = Field(default=120, ge=1, le=3600)


class BoardTaskHeartbeatRequest(BaseModel):
    agent_name: str
    claim_ttl_seconds: int = Field(default=120, ge=1, le=3600)


class BoardTaskReleaseRequest(BaseModel):
    agent_name: str
    status: str = "ready"


class BoardTaskStartRequest(BaseModel):
    agent_name: str


class BoardTaskCompleteRequest(BaseModel):
    agent_name: str
    metadata: dict[str, Any] = Field(default_factory=dict)


class BoardTaskBlockRequest(BaseModel):
    agent_name: str
    reason: Optional[str] = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class BoardTaskFailRequest(BaseModel):
    agent_name: str
    error: Optional[str] = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class BoardTaskCancelRequest(BaseModel):
    agent_name: Optional[str] = None
    reason: Optional[str] = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class BoardCommentCreateRequest(BaseModel):
    author: str
    comment_type: str = "info"
    body: str


class BoardCommentListResponse(BaseModel):
    comments: list["BoardCommentRecord"]


class BoardCommentRecord(BaseModel):
    id: str
    task_id: str
    author: str
    comment_type: str
    body: str
    created_at: datetime


class BoardEventListResponse(BaseModel):
    events: list["BoardEventRecord"]


class BoardEventRecord(BaseModel):
    id: str
    task_id: Optional[str] = None
    event_type: str
    actor: str
    payload: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime


class BoardTaskArtifactCreateRequest(BaseModel):
    created_by: str
    artifact_id: Optional[str] = None
    artifact_type: Optional[str] = None
    path: Optional[str] = None
    content_type: Optional[str] = None
    data_base64: Optional[str] = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class BoardTaskArtifactListResponse(BaseModel):
    artifacts: list["BoardTaskArtifactRecord"]


class BoardTaskArtifactRecord(BaseModel):
    id: str
    task_id: str
    artifact_id: Optional[str] = None
    artifact_type: Optional[str] = None
    path: Optional[str] = None
    metadata: dict[str, Any] = Field(default_factory=dict)
    created_by: Optional[str] = None
    created_at: datetime
