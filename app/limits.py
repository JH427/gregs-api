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
MAX_IMPORT_FILE_MB = _get_int("MAX_IMPORT_FILE_MB", 50)
EMBEDDING_MODEL_DEFAULT = os.getenv("EMBEDDING_MODEL_DEFAULT", "intfloat/e5-small-v2")
CHUNK_SIZE = _get_int("CHUNK_SIZE", 800)
CHUNK_OVERLAP = _get_int("CHUNK_OVERLAP", 120)
TOP_K_PER_DOMAIN = _get_int("TOP_K_PER_DOMAIN", 5)
KNOWLEDGE_QUERY_MAX_DOMAINS = _get_int("KNOWLEDGE_QUERY_MAX_DOMAINS", 6)
KNOWLEDGE_PROMOTION_MAX_INPUT_BYTES = _get_int("KNOWLEDGE_PROMOTION_MAX_INPUT_BYTES", 262144)
KNOWLEDGE_PROMOTION_MAX_CHUNKS = _get_int("KNOWLEDGE_PROMOTION_MAX_CHUNKS", 256)
KNOWLEDGE_PROMOTION_EMBED_BATCH_SIZE = _get_int("KNOWLEDGE_PROMOTION_EMBED_BATCH_SIZE", 16)
KNOWLEDGE_PROMOTION_UPSERT_BATCH_SIZE = _get_int("KNOWLEDGE_PROMOTION_UPSERT_BATCH_SIZE", 64)
KNOWLEDGE_PROMOTION_MODEL_INIT_TIMEOUT_SECONDS = _get_int("KNOWLEDGE_PROMOTION_MODEL_INIT_TIMEOUT_SECONDS", 60)
QDRANT_TIMEOUT_SECONDS = _get_int("QDRANT_TIMEOUT_SECONDS", 10)
QDRANT_HEALTH_TIMEOUT_SECONDS = _get_int("QDRANT_HEALTH_TIMEOUT_SECONDS", 2)


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
