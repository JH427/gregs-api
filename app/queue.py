import os
import time
import redis

REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
QUEUE_NAME = os.getenv("QUEUE_NAME", "task_queue")
PROCESSING_NAME = os.getenv("PROCESSING_NAME", "task_processing")
QUEUE_SET = os.getenv("QUEUE_SET", "task_queue:set")
WORKER_HEARTBEAT_KEY = os.getenv("WORKER_HEARTBEAT_KEY", "worker:heartbeat")


def get_redis() -> redis.Redis:
    return redis.Redis.from_url(REDIS_URL, decode_responses=True)


def enqueue_task(task_id: str) -> None:
    r = get_redis()
    if r.sadd(QUEUE_SET, task_id):
        r.rpush(QUEUE_NAME, task_id)


def ensure_enqueued(r: redis.Redis, task_id: str) -> None:
    if r.sadd(QUEUE_SET, task_id):
        r.rpush(QUEUE_NAME, task_id)


def dequeue_task(r: redis.Redis, timeout: int):
    return r.brpoplpush(QUEUE_NAME, PROCESSING_NAME, timeout=timeout)


def ack_task(r: redis.Redis, task_id: str) -> None:
    r.lrem(PROCESSING_NAME, 0, task_id)
    r.srem(QUEUE_SET, task_id)


def ack_processing(r: redis.Redis, task_id: str) -> None:
    r.lrem(PROCESSING_NAME, 0, task_id)


def requeue_inflight(r: redis.Redis) -> int:
    moved = 0
    while True:
        task_id = r.rpop(PROCESSING_NAME)
        if not task_id:
            break
        r.rpush(QUEUE_NAME, task_id)
        moved += 1
    return moved


def update_worker_heartbeat() -> None:
    r = get_redis()
    r.set(WORKER_HEARTBEAT_KEY, int(time.time()))
