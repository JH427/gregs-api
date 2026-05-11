# Task Runner + Knowledge API

FastAPI API + Redis queues + PostgreSQL persistence + search/fetch tasks + MinIO artifacts + Qdrant-backed knowledge promotion/query workflows.

The service is artifact-first: long-running work is queued, task outputs are stored as artifacts, and knowledge workflows promote selected artifacts into domain-scoped vector collections only when explicitly requested.

For agent onboarding and operational guidance, see [AGENT-README.md](./AGENT-README.md).

## Repo Layout
- `app/main.py` FastAPI API
- `app/models.py` SQLAlchemy models
- `app/db.py` DB setup + Alembic migration runner
- `app/db_helpers.py` Run/metric/note helpers
- `app/db_init.py` DB init script
- `app/queue.py` Redis queue helpers
- `app/cache.py` Search cache helpers
- `app/search.py` Search task logic + provider adapter
- `app/fetch.py` Fetch task logic + reader extraction + cache
- `app/imports.py` Import staging + parsing helpers
- `app/knowledge/` Domain, chunking, embedding, Qdrant, and promotion helpers
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
- `scripts/smoke_imports.sh` Import smoke tests
- `scripts/smoke_knowledge.sh` Knowledge promotion smoke tests
- `scripts/smoke_query.sh` Knowledge query smoke tests
- `scripts/smoke_board.sh` Board smoke tests
- `board_worker/` Distributed Hermes board worker
- `board-worker.example.yaml` Example board worker config

## Run Instructions
1. Build and start services:
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
- The API is published on the host at `127.0.0.1:8000`.
- PostgreSQL runs on the private Docker network only (no host ports).
- PostgreSQL data is stored on the host at `/mnt/data/postgres`.
- MinIO runs on the private Docker network only (no host ports).
- Qdrant runs on the private Docker network only (no host ports).
- Qdrant data is stored on the host at `/mnt/data/qdrant`.
- Hugging Face model/cache data is stored on the host at `/mnt/data/hf_cache`.
- For local testing from inside the stack, use `docker compose exec -T api ...` as shown below.
- Existing SQLite data is not migrated; Postgres starts empty.

## Configuration
PostgreSQL:
- `POSTGRES_USER` (default `api`)
- `POSTGRES_PASSWORD` (default `api`)
- `POSTGRES_DB` (default `api`)
- `DATABASE_URL` (default `postgresql+psycopg2://api:api@postgres:5432/api`)

Search:
- `BRAVE_API_KEY` (required for provider calls)
- `EXA_API_KEY` (required for Exa provider)
- `SEARCH_CACHE_TTL_SECONDS` (default `3600`)
- `SEARCH_MAX_RESULTS` (default `10`)
- `MAX_BATCH_SIZE` (default `50`)
- `EXA_API_URL` (optional, default `https://api.exa.ai/search`)

Fetch:
- `FETCH_CACHE_TTL_SECONDS` (default `3600`)
- `FETCH_DOMAIN_ALLOWLIST` (default `*`)

Imports:
- `MAX_IMPORT_FILE_MB` (default `50`)

Knowledge Promotion:
- `EMBEDDING_MODEL_DEFAULT` (default `intfloat/e5-small-v2`)
- `CHUNK_SIZE` (default `800`)
- `CHUNK_OVERLAP` (default `120`)
- `TOP_K_PER_DOMAIN` (default `5`)
- `KNOWLEDGE_QUERY_MAX_DOMAINS` (default `6`)
- `KNOWLEDGE_PROMOTION_MAX_INPUT_BYTES` (default `262144`)
- `KNOWLEDGE_PROMOTION_MAX_CHUNKS` (default `256`)
- `KNOWLEDGE_PROMOTION_EMBED_BATCH_SIZE` (default `16`)
- `KNOWLEDGE_PROMOTION_UPSERT_BATCH_SIZE` (default `64`)
- `KNOWLEDGE_PROMOTION_MODEL_INIT_TIMEOUT_SECONDS` (default `60`)
- `QDRANT_URL` (default `http://qdrant:6333`)
- `QDRANT_TIMEOUT_SECONDS` (default `10`)
- `QDRANT_HEALTH_TIMEOUT_SECONDS` (default `2`)

MinIO:
- `MINIO_ROOT_USER`
- `MINIO_ROOT_PASSWORD`
- `MINIO_BUCKET` (default `artifacts`)
- `MINIO_ENDPOINT` (default `minio:9000`)
- `MINIO_SECURE` (default `false`)

Board:
- `BOARD_ADMIN_TOKEN` (optional; enables admin bearer-token auth for `/api/board`)
- `BOARD_WORKER_TOKENS_JSON` (optional JSON object mapping bearer tokens to worker identities/capabilities)
- `BOARD_MAX_COMMENTS_PER_TASK` (default `12`)
- `BOARD_MAX_CHILD_TASKS` (default `3`)
- `BOARD_MAX_REASSIGNMENTS` (default `3`)
- `BOARD_CLAIM_TTL_SECONDS` (default `120`)
- `BOARD_AGENT_OFFLINE_AFTER_SECONDS` (default `90`)
- `BOARD_MAX_TASK_BODY_CHARS` (default `20000`)
- `BOARD_MAX_COMMENT_CHARS` (default `8000`)

## Migrations
Run migrations manually (optional, same as `make init-db`):
```bash
docker compose run --rm api alembic upgrade head
```

Rollback one migration:
```bash
docker compose run --rm api alembic downgrade -1
```

## Board Auth
Board auth stays open unless at least one of the following is configured:

- `BOARD_ADMIN_TOKEN`
- `BOARD_WORKER_TOKENS_JSON`

Example:
```bash
export BOARD_ADMIN_TOKEN=board-admin-dev
export BOARD_WORKER_TOKENS_JSON='{"rick-token":{"agent_name":"Rick","host_name":"titan","allowed_capabilities":["coordination"]}}'
docker compose up -d --build api
```

Example admin request:
```bash
docker compose exec -T api curl -s http://127.0.0.1:8000/api/board/tasks \
  -H "Authorization: Bearer ${BOARD_ADMIN_TOKEN}"
```

Worker tokens may only:
- register and heartbeat themselves
- claim tasks assigned to them or matching allowed capabilities
- comment as themselves on tasks they currently own
- complete, block, fail, release, start, and heartbeat tasks they currently own

## Board Worker
Example config file: [board-worker.example.yaml](./board-worker.example.yaml)

Run the worker:
```bash
python -m board_worker.runner --config board-worker.example.yaml
```

The worker uses stdlib HTTP and config parsing. The config format is a simple YAML-like `key: value` file with inline list support for `capabilities`.

## Ops Frontend
The operator UI is served separately from this repository under `/ops/`.

Board view:
- `/ops/board`

Console view:
- `/ops/`

The board UI talks to `/api/board/*` on the same origin. If board auth is enabled, enter a valid board bearer token in the board page session token field before using the board controls.

## Smoke Tests
Core task runner:
```bash
make smoke
```

Imports:
```bash
make smoke-imports
```

Knowledge promotion:
```bash
make smoke-knowledge
```

Knowledge query:
```bash
make smoke-query
```

Board:
```bash
make smoke-board
```

If board auth is enabled, provide a token for the board smoke:
```bash
BOARD_AUTH_TOKEN="${BOARD_ADMIN_TOKEN}" make smoke-board
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
