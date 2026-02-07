import json
import os
import re
import urllib.error
import urllib.request
from typing import Any, Dict, Optional, Tuple
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import redis
from bs4 import BeautifulSoup
from sqlalchemy.orm import Session

from app.artifacts import create_artifact_record
from app.cache import SEARCH_CACHE_TTL_SECONDS
from app.logging_utils import log_event
from app.limits import TASK_MAX_ARTIFACT_MB

FETCH_CACHE_TTL_SECONDS = int(os.getenv("FETCH_CACHE_TTL_SECONDS", str(SEARCH_CACHE_TTL_SECONDS)))
FETCH_CACHE_PREFIX = "fetch:v1:"


def canonicalize_url(url: str) -> str:
    parts = urlsplit(url)
    if parts.scheme not in {"http", "https"}:
        raise ValueError("invalid url scheme")
    netloc = parts.netloc.lower()
    if not netloc:
        raise ValueError("invalid url")
    host, sep, port = netloc.partition(":")
    if (parts.scheme == "http" and port == "80") or (parts.scheme == "https" and port == "443"):
        netloc = host
    path = parts.path or "/"
    if path != "/" and path.endswith("/"):
        path = path[:-1]
    query = urlencode(sorted(parse_qsl(parts.query, keep_blank_values=True)))
    return urlunsplit((parts.scheme, netloc, path, query, ""))


def normalize_fetch_params(params: Dict[str, Any]) -> Dict[str, Any]:
    url = params.get("url")
    if not isinstance(url, str) or not url.strip():
        raise ValueError("url is required")
    canonical_url = canonicalize_url(url.strip())
    reader_mode = params.get("reader_mode")
    reader_mode = True if reader_mode is None else bool(reader_mode)
    return {"canonical_url": canonical_url, "reader_mode": reader_mode}


def build_fetch_cache_key(canonical_url: str) -> str:
    return f"{FETCH_CACHE_PREFIX}{canonical_url}"


def get_fetch_cache(redis_client: redis.Redis, key: str) -> Optional[Dict[str, Any]]:
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


def set_fetch_cache(redis_client: redis.Redis, key: str, value: Dict[str, Any], ttl_seconds: int) -> None:
    redis_client.set(key, json.dumps(value), ex=ttl_seconds)


def _read_limited(resp, max_bytes: int) -> bytes:
    content_length = resp.headers.get("Content-Length")
    if content_length:
        try:
            if int(content_length) > max_bytes:
                raise ValueError("artifact_size_exceeded")
        except ValueError:
            pass
    data = bytearray()
    while True:
        chunk = resp.read(8192)
        if not chunk:
            break
        data.extend(chunk)
        if len(data) > max_bytes:
            raise ValueError("artifact_size_exceeded")
    return bytes(data)


def fetch_http(url: str, max_bytes: int, logger, task_id: str) -> Tuple[bytes, str, str, int]:
    req = urllib.request.Request(
        url,
        headers={"User-Agent": "Narsil-Fetch/1.0", "Accept": "text/html,application/xhtml+xml"},
    )
    with urllib.request.urlopen(req, timeout=20) as resp:
        status = getattr(resp, "status", 200)
        final_url = resp.geturl()
        content_type = resp.headers.get("Content-Type", "text/html")
        log_event(
            logger,
            "fetch_http_request",
            task_id=task_id,
            url=url,
            final_url=final_url,
            status=status,
            content_type=content_type,
        )
        body = _read_limited(resp, max_bytes)
    return body, content_type.split(";")[0].strip(), final_url, status


def _extract_metadata(soup: BeautifulSoup) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    title = None
    if soup.title and soup.title.string:
        title = soup.title.string.strip()
    h1 = soup.find("h1")
    if h1 and h1.get_text(strip=True):
        title = h1.get_text(strip=True)

    author = None
    for key in ("author", "article:author"):
        tag = soup.find("meta", attrs={"name": key}) or soup.find("meta", attrs={"property": key})
        if tag and tag.get("content"):
            author = tag["content"].strip()
            break

    published = None
    for key in ("article:published_time", "published_time", "published", "date"):
        tag = soup.find("meta", attrs={"property": key}) or soup.find("meta", attrs={"name": key})
        if tag and tag.get("content"):
            published = tag["content"].strip()
            break

    return title, author, published


def extract_readable_text(html: str, reader_mode: bool = True) -> Tuple[str, Optional[str], Optional[str], Optional[str]]:
    soup = BeautifulSoup(html, "lxml")
    for tag in soup(["script", "style", "noscript", "header", "footer", "nav", "aside", "form"]):
        tag.decompose()
    title, author, published = _extract_metadata(soup)
    container = soup.find("article") or soup.find("main") or soup.body or soup
    text = container.get_text(separator="\n", strip=True)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip(), title, author, published


def run_fetch_task(
    db: Session,
    task_id: str,
    params: Dict[str, Any],
    redis_client: redis.Redis,
    logger,
) -> Dict[str, Any]:
    normalized = normalize_fetch_params(params)
    canonical_url = normalized["canonical_url"]
    reader_mode = normalized["reader_mode"]
    store_raw_html = bool(params.get("store_raw_html", False))

    log_event(
        logger,
        "fetch_started",
        task_id=task_id,
        url=canonical_url,
        reader_mode=reader_mode,
        store_raw_html=store_raw_html,
    )

    cache_key = build_fetch_cache_key(canonical_url)
    cached = get_fetch_cache(redis_client, cache_key)
    if cached and "artifact_ids" in cached and "summary" in cached:
        log_event(logger, "fetch_cached_hit", task_id=task_id, url=canonical_url)
        log_event(logger, "fetch_completed", task_id=task_id, url=canonical_url, cached=True)
        return cached

    max_bytes = TASK_MAX_ARTIFACT_MB * 1024 * 1024

    try:
        body, content_type, final_url, status = fetch_http(canonical_url, max_bytes, logger, task_id)
        html_text = body.decode("utf-8", errors="ignore")
        text, title, author, published = extract_readable_text(html_text, reader_mode=reader_mode)
        if not text:
            raise ValueError("no_extractable_text")

        metadata_lines = []
        if title:
            metadata_lines.append(f"Title: {title}")
        if author:
            metadata_lines.append(f"Author: {author}")
        if published:
            metadata_lines.append(f"Published: {published}")
        metadata_lines.append(f"URL: {final_url}")
        if metadata_lines:
            metadata_lines.append("")
        content = "\n".join(metadata_lines) + text

        artifact_ids = []
        if store_raw_html:
            raw_artifact = create_artifact_record(
                db=db,
                task_id=task_id,
                artifact_type="fetch_html",
                content_type=content_type or "text/html",
                data=body,
                event_logger=logger,
            )
            artifact_ids.append(raw_artifact.id)

        text_artifact = create_artifact_record(
            db=db,
            task_id=task_id,
            artifact_type="fetch_text",
            content_type="text/plain",
            data=content.encode("utf-8"),
            event_logger=logger,
        )
        artifact_ids.insert(0, text_artifact.id)

        log_event(
            logger,
            "fetch_extraction_completed",
            task_id=task_id,
            url=canonical_url,
            text_bytes=len(content.encode("utf-8")),
        )

        summary = f"Fetched and extracted content from {urlsplit(canonical_url).hostname}"
        result_payload = {"artifact_ids": artifact_ids, "summary": summary}
        try:
            set_fetch_cache(redis_client, cache_key, result_payload, FETCH_CACHE_TTL_SECONDS)
        except Exception:
            pass
        log_event(logger, "fetch_completed", task_id=task_id, url=canonical_url, cached=False)
        return result_payload
    except Exception as exc:
        log_event(logger, "fetch_failed", task_id=task_id, url=canonical_url, error=str(exc))
        raise
