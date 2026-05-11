import json
import os
from dataclasses import dataclass, field
from typing import Any, Optional

from fastapi import Header, HTTPException


@dataclass(frozen=True)
class BoardActor:
    actor_type: str = "admin"
    name: str = "system"
    host: Optional[str] = None
    allowed_capabilities: list[str] = field(default_factory=list)
    token_id: Optional[str] = None


def _normalize_capabilities(values: list[Any]) -> list[str]:
    normalized: list[str] = []
    seen: set[str] = set()
    for value in values:
        item = " ".join(str(value).split()).strip()
        if not item:
            continue
        lowered = item.lower()
        if lowered in seen:
            continue
        seen.add(lowered)
        normalized.append(item)
    return normalized


def _load_worker_token_map() -> dict[str, dict[str, Any]]:
    raw = os.getenv("BOARD_WORKER_TOKENS_JSON", "").strip()
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
    except Exception as exc:
        raise RuntimeError(f"invalid BOARD_WORKER_TOKENS_JSON: {exc}")
    if not isinstance(parsed, dict):
        raise RuntimeError("invalid BOARD_WORKER_TOKENS_JSON: root must be an object")
    token_map: dict[str, dict[str, Any]] = {}
    for token, config in parsed.items():
        if not isinstance(token, str) or not token.strip() or not isinstance(config, dict):
            continue
        token_map[token] = config
    return token_map


def _authorization_token(authorization: Optional[str]) -> Optional[str]:
    if not authorization:
        return None
    scheme, _, value = authorization.partition(" ")
    if scheme.lower() != "bearer" or not value.strip():
        raise HTTPException(status_code=401, detail="invalid authorization header")
    return value.strip()


def is_admin_actor(actor: BoardActor) -> bool:
    return actor.actor_type == "admin"


def is_worker_actor(actor: BoardActor) -> bool:
    return actor.actor_type == "worker"


def require_admin_actor(actor: BoardActor) -> None:
    if not is_admin_actor(actor):
        raise HTTPException(status_code=403, detail="admin token required")


def require_worker_self(actor: BoardActor, agent_name: str) -> None:
    if is_admin_actor(actor):
        return
    if actor.name != agent_name:
        raise HTTPException(status_code=403, detail="worker token may only act as itself")


def worker_allows_capability(actor: BoardActor, requested_capability: Optional[str]) -> bool:
    if is_admin_actor(actor):
        return True
    if not requested_capability:
        return True
    allowed = {value.lower() for value in actor.allowed_capabilities}
    return requested_capability.lower() in allowed


def get_board_actor(authorization: Optional[str] = Header(default=None)) -> BoardActor:
    admin_token = os.getenv("BOARD_ADMIN_TOKEN", "").strip()
    worker_tokens = _load_worker_token_map()

    # Preserve current open behavior when no board auth is configured.
    if not admin_token and not worker_tokens:
        return BoardActor()

    token = _authorization_token(authorization)
    if not token:
        raise HTTPException(status_code=401, detail="authorization required")

    if admin_token and token == admin_token:
        return BoardActor(actor_type="admin", name="admin", token_id="admin")

    worker_config = worker_tokens.get(token)
    if worker_config is None:
        raise HTTPException(status_code=401, detail="invalid token")

    agent_name = str(worker_config.get("agent_name") or "").strip()
    host_name = str(worker_config.get("host_name") or "").strip()
    if not agent_name or not host_name:
        raise HTTPException(status_code=500, detail="invalid board worker token configuration")

    return BoardActor(
        actor_type="worker",
        name=agent_name,
        host=host_name,
        allowed_capabilities=_normalize_capabilities(worker_config.get("allowed_capabilities") or []),
        token_id=agent_name,
    )
