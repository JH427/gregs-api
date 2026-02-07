import os
from typing import List, Tuple
from urllib.parse import urlparse

from app.queue import get_redis


def _get_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError:
        return default


def _get_list(name: str, default: List[str]) -> List[str]:
    raw = os.getenv(name)
    if raw is None:
        return default
    items = [item.strip().lower() for item in raw.split(",") if item.strip()]
    return items or default


TASK_MAX_RUNTIME_SECONDS = _get_int("TASK_MAX_RUNTIME_SECONDS", 600)
TASK_MAX_ARTIFACT_MB = _get_int("TASK_MAX_ARTIFACT_MB", 25)
TASKS_PER_MINUTE = _get_int("TASKS_PER_MINUTE", 30)
SEARCH_SOURCE_ALLOWLIST = _get_list("SEARCH_SOURCE_ALLOWLIST", ["brave"])
FETCH_DOMAIN_ALLOWLIST = os.getenv("FETCH_DOMAIN_ALLOWLIST", "*")
MAX_BATCH_SIZE = _get_int("MAX_BATCH_SIZE", 50)


RATE_LIMIT_LUA = """
local key = KEYS[1]
local limit = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local current = redis.call('INCR', key)
if current == 1 then
  redis.call('EXPIRE', key, window)
end
return current
"""


def rate_limit_check(identity: str = "global") -> Tuple[bool, int]:
    r = get_redis()
    key = f"rate_limit:{identity}"
    current = r.eval(RATE_LIMIT_LUA, 1, key, TASKS_PER_MINUTE, 60)
    return current <= TASKS_PER_MINUTE, int(current)


def search_sources_allowed(sources: List[str]) -> bool:
    allowed = set(SEARCH_SOURCE_ALLOWLIST)
    return all(source.lower() in allowed for source in sources)


def validate_fetch_domain(url: str) -> bool:
    if FETCH_DOMAIN_ALLOWLIST.strip() == "*":
        return True
    try:
        domain = urlparse(url).hostname or ""
    except Exception:
        return False
    if not domain:
        return False
    allowed = {d.strip().lower() for d in FETCH_DOMAIN_ALLOWLIST.split(",") if d.strip()}
    return domain.lower() in allowed
