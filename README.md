# Task Runner + Queue (Phase 4)

FastAPI API + worker + Redis queue + PostgreSQL persistence + Search Aggregator + MinIO artifact store.

## Repo Layout
- `app/main.py` FastAPI API
- `app/models.py` SQLAlchemy models
- `app/db.py` DB setup + Alembic migration runner
- `app/db_helpers.py` Run/metric/note helpers
- `app/db_init.py` DB init script
- `app/queue.py` Redis queue helpers
- `app/cache.py` Search cache helpers
- `app/search.py` Search task logic + provider adapter
- `app/artifacts.py` Artifact API + DB helpers
- `app/storage.py` MinIO client helpers
- `app/logging_utils.py` JSON logging helpers
- `app/logging_config.json` Logging config
- `worker/worker.py` Worker process
- `alembic/` Alembic migrations
- `alembic.ini` Alembic config
- `docker-compose.yml` Local deployment
- `Makefile` Convenience commands
- `scripts/smoke.sh` Smoke tests

## Run Instructions
1. Build and start services (includes Postgres):
   ```bash
   cd /home/adminuser/api
   make start
   ```

2. Run migrations:
   ```bash
   make init-db
   ```

3. Check health from inside the API container:
   ```bash
   docker compose exec -T api curl -s http://127.0.0.1:8000/health
   ```

Notes:
- The API binds to `127.0.0.1` inside the container and is not published on the host.
- PostgreSQL runs on the private Docker network only (no host ports).
- PostgreSQL data is stored on the host at `/mnt/data/postgres`.
- MinIO runs on the private Docker network only (no host ports).
- For local testing, use `docker compose exec -T api ...` as shown above.
- Existing SQLite data is not migrated; Postgres starts empty.

## Configuration
PostgreSQL:
- `POSTGRES_USER` (default `api`)
- `POSTGRES_PASSWORD` (default `api`)
- `POSTGRES_DB` (default `api`)
- `DATABASE_URL` (default `postgresql+psycopg2://api:api@postgres:5432/api`)

Search:
- `BRAVE_API_KEY` (required for provider calls)
- `SEARCH_CACHE_TTL_SECONDS` (default `3600`)
- `SEARCH_MAX_RESULTS` (default `10`)

MinIO:
- `MINIO_ROOT_USER`
- `MINIO_ROOT_PASSWORD`
- `MINIO_BUCKET` (default `artifacts`)
- `MINIO_ENDPOINT` (default `minio:9000`)
- `MINIO_SECURE` (default `false`)

## Migrations
Run migrations manually (optional, same as `make init-db`):
```bash
docker compose run --rm api alembic upgrade head
```

Rollback one migration:
```bash
docker compose run --rm api alembic downgrade -1
```

## Verify Postgres Is In Use
List tables:
```bash
docker compose exec -T postgres psql -U ${POSTGRES_USER:-api} -d ${POSTGRES_DB:-api} -c "\\dt"
```

Check tasks table exists:
```bash
docker compose exec -T postgres psql -U ${POSTGRES_USER:-api} -d ${POSTGRES_DB:-api} -c "select count(*) from tasks;"
```

## Helper Usage Examples
`has_been_processed` and `latest_run_for_key` are DB-layer helpers (not API endpoints).

```python
from app.db import SessionLocal
from app.db_helpers import has_been_processed, latest_run_for_key

db = SessionLocal()
try:
    # scope prefixes the key (e.g. scope="search", key="<hash>")
    processed, last_at = has_been_processed(db, key="abc123", scope="search")
    print(processed, last_at)

    run = latest_run_for_key(db, "search:abc123")
    print(run.id if run else None)
finally:
    db.close()
```

## Curl Examples (run inside API container)
Create echo task:
```bash
docker compose exec -T api curl -s -X POST http://127.0.0.1:8000/tasks \
  -H 'Content-Type: application/json' \
  -d '{"type":"echo","params":{"msg":"hello"},"timeout_seconds":300,"max_retries":0}'
```

Get task status:
```bash
docker compose exec -T api curl -s http://127.0.0.1:8000/tasks/<task_id>
```

Create sleep task:
```bash
docker compose exec -T api curl -s -X POST http://127.0.0.1:8000/tasks \
  -H 'Content-Type: application/json' \
  -d '{"type":"sleep","params":{"seconds":10},"timeout_seconds":300,"max_retries":0}'
```

Cancel task:
```bash
docker compose exec -T api curl -s -X POST http://127.0.0.1:8000/tasks/<task_id>/cancel
```

Submit search task:
```bash
docker compose exec -T api curl -s -X POST http://127.0.0.1:8000/search \
  -H 'Content-Type: application/json' \
  -d '{"query":"open source task runner","sources":["brave"],"recency_days":7}'
```

Poll for results:
```bash
docker compose exec -T api curl -s http://127.0.0.1:8000/tasks/<task_id>
```

Create an artifact (JSON + base64):
```bash
docker compose exec -T api curl -s -X POST http://127.0.0.1:8000/artifacts \
  -H 'Content-Type: application/json' \
  -d '{"task_id":"<task_id>","type":"log","content_type":"text/plain","data_base64":"SGVsbG8gd29ybGQ="}'
```

Fetch artifact metadata:
```bash
docker compose exec -T api curl -s http://127.0.0.1:8000/artifacts/<artifact_id>/meta
```

Download artifact by ID:
```bash
docker compose exec -T api curl -s http://127.0.0.1:8000/artifacts/<artifact_id>
```

## Smoke Tests
```bash
make smoke
```

## Phase 7 — Safety & Limits
Limits are enforced centrally and read once at startup from environment variables.

Defaults (override via env):
- `TASK_MAX_RUNTIME_SECONDS=600`
- `TASK_MAX_ARTIFACT_MB=25`
- `TASKS_PER_MINUTE=30`
- `SEARCH_SOURCE_ALLOWLIST=brave`
- `FETCH_DOMAIN_ALLOWLIST=*`
- `MAX_BATCH_SIZE=50`

Enforcement:
- Worker kills tasks exceeding runtime (error: `task_runtime_exceeded`, log event `task_killed`).
- Artifacts exceeding size limit are rejected (error: `artifact_size_exceeded`, log event `artifact_rejected`).
- Search sources not in allowlist are rejected (`task_rejected_invalid_params`).
- Enqueue is rate limited (HTTP 429, `rate_limit_exceeded`).

Limits introspection:
- `GET /limits` returns the effective limits.
