import io
import os
from typing import BinaryIO

from minio import Minio

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "artifacts")
MINIO_SECURE = os.getenv("MINIO_SECURE", "false").lower() == "true"


def get_client() -> Minio:
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE,
    )


def ensure_bucket(client: Minio) -> None:
    if not client.bucket_exists(MINIO_BUCKET):
        client.make_bucket(MINIO_BUCKET)


def put_bytes(client: Minio, path: str, data: bytes, content_type: str) -> None:
    ensure_bucket(client)
    stream: BinaryIO = io.BytesIO(data)
    client.put_object(
        MINIO_BUCKET,
        path,
        stream,
        length=len(data),
        content_type=content_type,
    )


def get_object(client: Minio, path: str):
    return client.get_object(MINIO_BUCKET, path)
