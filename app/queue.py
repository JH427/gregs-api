import os
import time
import redis

REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
QUEUE_QUERY = os.getenv("QUEUE_QUERY", "queue_query")
QUEUE_PROMOTE = os.getenv("QUEUE_PROMOTE", "queue_promote")
QUEUE_NAME = QUEUE_QUERY
PROCESSING_NAME = f"{QUEUE_NAME}:processing"
QUEUE_SET = f"{QUEUE_NAME}:set"
WORKER_HEARTBEAT_KEY = os.getenv("WORKER_HEARTBEAT_KEY", "worker:heartbeat")


def get_redis() -> redis.Redis:
    return redis.Redis.from_url(REDIS_URL, decode_responses=True)


def get_queue_for_task(task_type: str) -> str:
    if task_type == "knowledge_promote":
        return QUEUE_PROMOTE
    return QUEUE_QUERY


def get_processing_name(queue_name: str) -> str:
    return f"{queue_name}:processing"


def get_queue_set(queue_name: str) -> str:
    return f"{queue_name}:set"


def enqueue_task(task_id: str, task_type: str) -> None:
    r = get_redis()
    queue_name = get_queue_for_task(task_type)
    queue_set = get_queue_set(queue_name)
    if r.sadd(queue_set, task_id):
        r.rpush(queue_name, task_id)


def ensure_enqueued(r: redis.Redis, task_id: str, task_type: str) -> None:
    queue_name = get_queue_for_task(task_type)
    queue_set = get_queue_set(queue_name)
    if r.sadd(queue_set, task_id):
        r.rpush(queue_name, task_id)


def dequeue_task(r: redis.Redis, timeout: int, queue_name: str):
    return r.brpoplpush(queue_name, get_processing_name(queue_name), timeout=timeout)


def ack_task(r: redis.Redis, task_id: str, queue_name: str) -> None:
    r.lrem(get_processing_name(queue_name), 0, task_id)
    r.srem(get_queue_set(queue_name), task_id)


def ack_processing(r: redis.Redis, task_id: str, queue_name: str) -> None:
    r.lrem(get_processing_name(queue_name), 0, task_id)


def requeue_inflight(r: redis.Redis, queue_name: str) -> int:
    processing_name = get_processing_name(queue_name)
    moved = 0
    while True:
        task_id = r.rpop(processing_name)
        if not task_id:
            break
        r.rpush(queue_name, task_id)
        moved += 1
    return moved


def update_worker_heartbeat() -> None:
    r = get_redis()
    r.set(WORKER_HEARTBEAT_KEY, int(time.time()))
