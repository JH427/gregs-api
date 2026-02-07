import hashlib
import json
import os
import urllib.parse
import urllib.request
from datetime import datetime
from typing import Any, Dict, List, Tuple

import redis
from sqlalchemy.orm import Session

from app.artifacts import create_artifact_record
from app.cache import SEARCH_CACHE_TTL_SECONDS, build_cache_key, get_cache, set_cache
from app.logging_utils import log_event

BRAVE_API_URL = os.getenv("BRAVE_API_URL", "https://api.search.brave.com/res/v1/web/search")
BRAVE_API_KEY = os.getenv("BRAVE_API_KEY")
SEARCH_MAX_RESULTS = int(os.getenv("SEARCH_MAX_RESULTS", "10"))


class SearchProvider:
    name = ""

    def search(self, query: str, recency_days: int) -> Dict[str, Any]:
        raise NotImplementedError


class BraveSearchProvider(SearchProvider):
    name = "brave"

    def search(self, query: str, recency_days: int) -> Dict[str, Any]:
        if not BRAVE_API_KEY:
            raise RuntimeError("BRAVE_API_KEY is not set")

        params = {
            "q": query,
            "count": str(SEARCH_MAX_RESULTS),
        }
        freshness = _recency_to_freshness(recency_days)
        if freshness:
            params["freshness"] = freshness

        url = f"{BRAVE_API_URL}?{urllib.parse.urlencode(params)}"
        req = urllib.request.Request(
            url,
            headers={
                "Accept": "application/json",
                "X-Subscription-Token": BRAVE_API_KEY,
            },
        )
        with urllib.request.urlopen(req, timeout=15) as resp:
            body = resp.read().decode("utf-8")
        return json.loads(body)


def normalize_params(params: Dict[str, Any]) -> Dict[str, Any]:
    query = params.get("query")
    if not isinstance(query, str) or not query.strip():
        raise ValueError("query is required")
    query = " ".join(query.split()).strip()

    sources = params.get("sources")
    if sources is None:
        sources = ["brave"]
    if not isinstance(sources, list) or not sources:
        raise ValueError("sources must be a non-empty list")
    normalized_sources = []
    for source in sources:
        if not isinstance(source, str) or not source.strip():
            raise ValueError("sources must contain non-empty strings")
        normalized_sources.append(source.strip().lower())

    recency_days = params.get("recency_days", 7)
    if recency_days is None:
        recency_days = 7
    if not isinstance(recency_days, int) or recency_days < 0:
        raise ValueError("recency_days must be a non-negative integer")

    return {
        "query": query,
        "sources": sorted(set(normalized_sources)),
        "recency_days": recency_days,
    }


def compute_cache_key(query: str, sources: List[str], recency_days: int) -> str:
    cache_input = f"{query}|{','.join(sorted(sources))}|{recency_days}"
    return hashlib.sha256(cache_input.encode("utf-8")).hexdigest()


def run_search_task(
    db: Session,
    task_id: str,
    params: Dict[str, Any],
    redis_client: redis.Redis,
    logger,
) -> Dict[str, Any]:
    normalized = normalize_params(params)
    query = normalized["query"]
    sources = normalized["sources"]
    recency_days = normalized["recency_days"]

    log_event(
        logger,
        "search_task_started",
        task_id=task_id,
        query=query,
        sources=sources,
        recency_days=recency_days,
    )

    cache_key = build_cache_key(compute_cache_key(query, sources, recency_days))
    cached_value = get_cache(redis_client, cache_key)
    if cached_value and "artifact_ids" in cached_value and "summary" in cached_value:
        log_event(logger, "search_cache_hit", task_id=task_id, cache_key=cache_key)
        log_event(logger, "search_task_completed", task_id=task_id, cache_key=cache_key, cached=True)
        return cached_value

    log_event(logger, "search_cache_miss", task_id=task_id, cache_key=cache_key)

    try:
        results, raw_payloads, sources_used = call_providers(query, sources, recency_days, logger)
        artifact_ids = []
        for provider_name, raw_payload in raw_payloads:
            data = json.dumps(raw_payload).encode("utf-8")
            artifact = create_artifact_record(
                db=db,
                task_id=task_id,
                artifact_type="search_raw",
                content_type="application/json",
                data=data,
                event_logger=logger,
            )
            artifact_ids.append(artifact.id)

        summary = f"{len(results)} results from {', '.join(sources_used)}"
        normalized_payload = {
            "query": query,
            "results": results,
            "sources_used": sources_used,
            "retrieved_at": datetime.utcnow().isoformat() + "Z",
        }
        normalized_artifact = create_artifact_record(
            db=db,
            task_id=task_id,
            artifact_type="search_results",
            content_type="application/json",
            data=json.dumps(normalized_payload).encode("utf-8"),
            event_logger=logger,
        )

        result_payload = {
            "artifact_ids": [normalized_artifact.id] + artifact_ids,
            "summary": summary,
        }
        try:
            set_cache(redis_client, cache_key, result_payload, SEARCH_CACHE_TTL_SECONDS)
        except Exception:
            pass

        log_event(logger, "search_task_completed", task_id=task_id, cache_key=cache_key, cached=False)
        return result_payload
    except Exception as exc:
        log_event(logger, "search_task_failed", task_id=task_id, error=str(exc))
        raise


def call_providers(
    query: str,
    sources: List[str],
    recency_days: int,
    logger,
) -> Tuple[List[Dict[str, Any]], List[Tuple[str, Dict[str, Any]]], List[str]]:
    results: List[Dict[str, Any]] = []
    raw_payloads: List[Tuple[str, Dict[str, Any]]] = []
    sources_used: List[str] = []

    for source in sources:
        if source == "brave":
            provider = BraveSearchProvider()
        else:
            raise RuntimeError(f"unsupported search provider: {source}")

        log_event(logger, "search_provider_called", provider=provider.name, query=query)
        raw = provider.search(query, recency_days)
        raw_payloads.append((provider.name, raw))
        normalized = normalize_brave_results(raw)
        results.extend(normalized)
        sources_used.append(provider.name)

    return results, raw_payloads, sources_used


def normalize_brave_results(raw: Dict[str, Any]) -> List[Dict[str, Any]]:
    items = raw.get("web", {}).get("results", [])
    normalized: List[Dict[str, Any]] = []
    for item in items:
        title = item.get("title")
        url = item.get("url") or item.get("link")
        snippet = item.get("description") or item.get("snippet")
        if not title or not url:
            continue
        normalized.append(
            {
                "title": title,
                "url": url,
                "snippet": snippet or "",
                "source": "brave",
            }
        )
    return normalized


def _recency_to_freshness(recency_days: int) -> str:
    if recency_days <= 0:
        return ""
    if recency_days <= 1:
        return "pd"
    if recency_days <= 7:
        return "pw"
    if recency_days <= 31:
        return "pm"
    return "py"
