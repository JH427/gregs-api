import os
from typing import Sequence

from qdrant_client import QdrantClient, models

from app.knowledge.domains import KnowledgeDomain
from app.limits import QDRANT_TIMEOUT_SECONDS

QDRANT_URL = os.getenv("QDRANT_URL", "http://qdrant:6333")


def get_qdrant_client(timeout: int | float | None = None) -> QdrantClient:
    return QdrantClient(url=QDRANT_URL, timeout=timeout or QDRANT_TIMEOUT_SECONDS)


def close_qdrant_client(client: QdrantClient) -> None:
    close = getattr(client, "close", None)
    if callable(close):
        close()


def collection_name_for_domain(domain: KnowledgeDomain | str) -> str:
    return f"knowledge_{KnowledgeDomain(domain).value}"


def _vector_size_from_collection(info) -> int | None:
    vectors = info.config.params.vectors
    if isinstance(vectors, dict):
        first = next(iter(vectors.values()), None)
        return getattr(first, "size", None)
    return getattr(vectors, "size", None)


def ensure_collection(
    domain: KnowledgeDomain | str,
    vector_size: int,
    distance: models.Distance = models.Distance.COSINE,
) -> str:
    client = get_qdrant_client()
    try:
        collection_name = collection_name_for_domain(domain)
        if not client.collection_exists(collection_name):
            client.create_collection(
                collection_name=collection_name,
                vectors_config=models.VectorParams(size=vector_size, distance=distance),
            )
            return collection_name

        info = client.get_collection(collection_name)
        existing_size = _vector_size_from_collection(info)
        if existing_size is not None and int(existing_size) != int(vector_size):
            raise ValueError(
                f"qdrant collection {collection_name} already exists with vector size {existing_size}, expected {vector_size}"
            )
        return collection_name
    finally:
        close_qdrant_client(client)


def upsert_points(domain: KnowledgeDomain | str, points: Sequence[models.PointStruct]) -> None:
    if not points:
        return
    client = get_qdrant_client()
    try:
        client.upsert(
            collection_name=collection_name_for_domain(domain),
            points=list(points),
            wait=True,
        )
    finally:
        close_qdrant_client(client)


def delete_points(domain: KnowledgeDomain | str, point_ids: Sequence[str]) -> None:
    if not point_ids:
        return
    client = get_qdrant_client()
    try:
        client.delete(
            collection_name=collection_name_for_domain(domain),
            points_selector=models.PointIdsList(points=list(point_ids)),
            wait=True,
        )
    finally:
        close_qdrant_client(client)


def collection_exists(domain: KnowledgeDomain | str) -> bool:
    client = get_qdrant_client()
    try:
        return client.collection_exists(collection_name_for_domain(domain))
    finally:
        close_qdrant_client(client)
