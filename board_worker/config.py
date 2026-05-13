import os
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class BoardWorkerConfig:
    agent_name: str
    host_name: str
    api_url: str
    api_token: str
    canonical_agent_name: str = ""
    cf_access_client_id: str = ""
    cf_access_client_secret: str = ""
    poll_interval_seconds: int = 10
    heartbeat_interval_seconds: int = 20
    claim_ttl_seconds: int = 120
    max_parallel_tasks: int = 1
    workspace_root: str = "~/agent-workspaces"
    hermes_command: str = "hermes"
    hermes_profile: str = "Rick"
    capabilities: list[str] = field(default_factory=list)


def _parse_scalar(raw: str):
    value = os.path.expandvars(raw.strip())
    if value.startswith(("'", '"')) and value.endswith(("'", '"')) and len(value) >= 2:
        value = value[1:-1]
    lower = value.lower()
    if lower in {"true", "false"}:
        return lower == "true"
    if lower in {"null", "none", "~", ""}:
        return None
    if value.startswith("[") and value.endswith("]"):
        inner = value[1:-1].strip()
        if not inner:
            return []
        return [item.strip().strip("'\"") for item in inner.split(",") if item.strip()]
    try:
        return int(value)
    except ValueError:
        return value


def load_config(path: str) -> BoardWorkerConfig:
    values: dict[str, object] = {}
    for raw_line in Path(path).read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if ":" not in line:
            raise ValueError(f"invalid config line: {raw_line}")
        key, value = line.split(":", 1)
        values[key.strip()] = _parse_scalar(value)

    required = ["agent_name", "host_name", "api_url", "api_token"]
    missing = [key for key in required if not values.get(key)]
    if missing:
        raise ValueError(f"missing config keys: {', '.join(missing)}")

    api_url = str(values["api_url"]).rstrip("/")
    workspace_root = os.path.expanduser(str(values.get("workspace_root") or "~/agent-workspaces"))
    capabilities = values.get("capabilities")
    if capabilities is None:
        capabilities = []
    if not isinstance(capabilities, list):
        raise ValueError("capabilities must be a list")

    return BoardWorkerConfig(
        agent_name=str(values["agent_name"]),
        host_name=str(values["host_name"]),
        api_url=api_url,
        api_token=str(values["api_token"]),
        canonical_agent_name=str(values.get("canonical_agent_name") or values["agent_name"]),
        cf_access_client_id=str(values.get("cf_access_client_id") or ""),
        cf_access_client_secret=str(values.get("cf_access_client_secret") or ""),
        poll_interval_seconds=int(values.get("poll_interval_seconds") or 10),
        heartbeat_interval_seconds=int(values.get("heartbeat_interval_seconds") or 20),
        claim_ttl_seconds=int(values.get("claim_ttl_seconds") or 120),
        max_parallel_tasks=int(values.get("max_parallel_tasks") or 1),
        workspace_root=workspace_root,
        hermes_command=str(values.get("hermes_command") or "hermes"),
        hermes_profile=str(values.get("hermes_profile") or "Rick"),
        capabilities=[str(item) for item in capabilities],
    )
