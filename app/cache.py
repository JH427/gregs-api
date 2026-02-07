import json
import os
from typing import Any, Dict, Optional

import redis

from app.queue import get_redis

CACHE_PREFIX = "search:v1:"
SEARCH_CACHE_TTL_SECONDS = int(os.getenv("SEARCH_CACHE_TTL_SECONDS", "3600"))


def build_cache_key(key_hash: str) -> str:
    return f"{CACHE_PREFIX}{key_hash}"


def get_cache(redis_client: redis.Redis, key: str) -> Optional[Dict[str, Any]]:
    try:
        payload = redis_client.get(key)
    except Exception:
        return None
    if not payload:
        return None
    try:
        return json.loads(payload)
    except Exception:
        return None


def set_cache(redis_client: redis.Redis, key: str, value: Dict[str, Any], ttl_seconds: int) -> None:
    redis_client.set(key, json.dumps(value), ex=ttl_seconds)


def get_cache_client() -> redis.Redis:
    return get_redis()
