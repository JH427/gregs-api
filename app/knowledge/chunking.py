from typing import Callable, Dict, List, Optional

from app.limits import CHUNK_OVERLAP, CHUNK_SIZE

CHUNKER_VERSION = "v1"


def normalize_chunk_params(params: Optional[Dict[str, int]] = None) -> Dict[str, int]:
    params = params or {}
    chunk_size = int(params.get("chunk_size", CHUNK_SIZE))
    overlap = int(params.get("overlap", CHUNK_OVERLAP))
    if chunk_size <= 0:
        raise ValueError("chunk_size must be positive")
    if overlap < 0:
        raise ValueError("overlap must be non-negative")
    if overlap >= chunk_size:
        raise ValueError("overlap must be smaller than chunk_size")
    return {"chunk_size": chunk_size, "overlap": overlap}


def chunk_text(
    text: str,
    chunk_size: int = CHUNK_SIZE,
    overlap: int = CHUNK_OVERLAP,
    should_continue: Optional[Callable[[], None]] = None,
) -> List[Dict[str, object]]:
    normalized = normalize_chunk_params({"chunk_size": chunk_size, "overlap": overlap})
    if not isinstance(text, str) or not text.strip():
        return []

    size = normalized["chunk_size"]
    overlap_chars = normalized["overlap"]
    step = size - overlap_chars

    chunks: List[Dict[str, object]] = []
    chunk_index = 0
    start = 0
    length = len(text)
    while start < length:
        if should_continue:
            should_continue()
        end = min(length, start + size)
        chunk_body = text[start:end]
        if chunk_body.strip():
            chunks.append(
                {
                    "chunk_index": chunk_index,
                    "start": start,
                    "end": end,
                    "text": chunk_body,
                }
            )
            chunk_index += 1
        if end >= length:
            break
        start += step
    return chunks
