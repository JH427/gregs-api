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
- `EMBEDDING_MODEL_DEFAULT` (default `intfloat/e5-small`)
- `CHUNK_SIZE` (default `800`)
- `CHUNK_OVERLAP` (default `120`)
- `QDRANT_URL` (default `http://qdrant:6333`)

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

Submit Exa search task:
```bash
docker compose exec -T api curl -s -X POST http://127.0.0.1:8000/search \
  -H 'Content-Type: application/json' \
  -d '{"query":"agent memory systems","sources":["exa"],"recency_days":7}'
```

Batch search results are stored in an artifact with type `search_batch_results` (grouped results per query).

Submit batch search task:
```bash
docker compose exec -T api curl -s -X POST http://127.0.0.1:8000/search/batch \
  -H 'Content-Type: application/json' \
  -d '{"queries":["task runner open source","queue worker redis"],"sources":["brave"],"recency_days":7}'
```

Poll for results:
```bash
docker compose exec -T api curl -s http://127.0.0.1:8000/tasks/<task_id>
```

Submit fetch task:
```bash
docker compose exec -T api curl -s -X POST http://127.0.0.1:8000/fetch \
  -H 'Content-Type: application/json' \
  -d '{"url":"https://example.com/article","reader_mode":true,"store_raw_html":true}'
```

## Phase 9A — Imports (Artifacts Only)
Phase 9A keeps imports artifact-first. Raw inputs are preserved, parsed artifacts are derived and traceable, and this phase does not add embeddings, vector storage, summarization, classification, or promotion.

Behavior:
- `POST /import/files` accepts PDF, TXT, and MD uploads and queues an async import task.
- PDFs are parsed with MarkItDown into Markdown artifacts.
- TXT and MD files are read directly as UTF-8 text.
- `POST /import/chatgpt` ingests ChatGPT export JSON and preserves per-conversation structure.
- All successful imports emit `import_raw`, parsed artifacts, and `import_metadata`; handled failures emit `import_error`.

Upload a file:
```bash
docker compose exec -T api curl -s -X POST http://127.0.0.1:8000/import/files \
  -F 'file=@/tmp/sample.pdf'
```

Upload a ChatGPT export:
```bash
docker compose exec -T api curl -s -X POST http://127.0.0.1:8000/import/chatgpt \
  -H 'Content-Type: application/json' \
  --data-binary @/tmp/chatgpt-export.json
```

Poll the queued task:
```bash
docker compose exec -T api curl -s http://127.0.0.1:8000/tasks/<task_id>
```

## Phase 9B — Knowledge Promotion
Phase 9B promotes existing artifacts into domain-scoped vector memory. Promotion is explicit, embeddings are created only during promotion, Qdrant stays on the private Docker network, and BELIEF promotion is blocked in this phase.

Behavior:
- `POST /knowledge/promote` queues an async promotion task.
- Promotion resolves text from an existing artifact, chunks it deterministically, embeds it locally on CPU, and stores vectors in Qdrant.
- Postgres stores promotion metadata and chunk references, while chunk text itself stays in artifacts.
- The same artifact plus the same model and chunk config dedupes via `promotion_key` instead of re-embedding.

Promote an artifact:
```bash
docker compose exec -T api curl -s -X POST http://127.0.0.1:8000/knowledge/promote \
  -H 'Content-Type: application/json' \
  -d '{"artifact_id":"<artifact_id>","domain":"project","source":"files","confidence":"medium"}'
```

## Phase 9C — Knowledge Query
Phase 9C queries promoted knowledge asynchronously and remains artifact-first. One artifact contains ranked results, and a second audit artifact records the searched domains, routing behavior, filters, and scoring decisions that produced those results.

Behavior:
- `POST /knowledge/query` queues an async query task.
- Explicit domains override the router; omitted domains trigger a deterministic rules-only router.
- Results are merged across searched domains with precedence weighting and optional prefer boosts.
- Zero-result queries still succeed and still create both artifacts.

Query promoted knowledge:
```bash
docker compose exec -T api curl -s -X POST http://127.0.0.1:8000/knowledge/query \
  -H 'Content-Type: application/json' \
  -d '{"query":"Brave batch is failing, what was the decision?","domains":["ops","project"],"domain_mode":"prefer","top_k_per_domain":5}'
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

List artifacts for a task:
```bash
docker compose exec -T api curl -s http://127.0.0.1:8000/tasks/<task_id>/artifacts
```

List artifacts (global index):
```bash
docker compose exec -T api curl -s "http://127.0.0.1:8000/artifacts?type=fetch_text&limit=50"
```

Download artifact by ID:
```bash
docker compose exec -T api curl -s http://127.0.0.1:8000/artifacts/<artifact_id>
```

## Smoke Tests
```bash
make smoke
```

```bash
make smoke-imports
```

```bash
make smoke-knowledge
```

```bash
make smoke-query
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
- `MAX_IMPORT_FILE_MB=50`
- `EMBEDDING_MODEL_DEFAULT=intfloat/e5-small`
- `CHUNK_SIZE=800`
- `CHUNK_OVERLAP=120`
- `TOP_K_PER_DOMAIN=5`
- `KNOWLEDGE_QUERY_MAX_DOMAINS=6`

Enforcement:
- Worker kills tasks exceeding runtime (error: `task_runtime_exceeded`, log event `task_killed`).
- Artifacts exceeding size limit are rejected (error: `artifact_size_exceeded`, log event `artifact_rejected`).
- Search sources not in allowlist are rejected (`task_rejected_invalid_params`).
- Enqueue is rate limited (HTTP 429, `rate_limit_exceeded`).

Limits introspection:
- `GET /limits` returns the effective limits.

## Phase 8A — Batch Search
Batch search lets you submit multiple queries in one request while still executing asynchronously.

Behavior:
- One request creates one task of type `search_batch`.
- Results are grouped into a single artifact with type `search_batch_results`.
- Per-query cache reuse uses the same cache key as single-search, so overlapping batches benefit from cached results.
- Identical batch requests dedupe to the same artifacts (fast convergence).
- Batch size is enforced by `MAX_BATCH_SIZE` (1–50).

Endpoint:
```bash
POST /search/batch
```

Request:
```json
{
  "queries": ["query one", "query two"],
  "sources": ["brave"],
  "recency_days": 7
}
```

Result:
- `GET /tasks/{id}` returns `result.artifact_ids` with the grouped `search_batch_results` artifact plus any per-query artifacts.

## Phase 8A.1 — Exa Provider
Exa is available as an additional provider when explicitly requested.

Usage:
- Include `"exa"` in `sources` and add `EXA_API_KEY` to the environment.
- Keep `SEARCH_SOURCE_ALLOWLIST` updated (e.g. `brave,exa`) to allow Exa.

Provider guidance:
- **Exa** is best for high-signal research and cleaner results.
- **Brave** is better for broad recon and coverage.

Recency notes:
- For Exa, `recency_days` is mapped to `startPublishedDate` (best-effort).
- If `recency_days <= 0`, Exa does not apply a published-date filter.

## Phase 8B — Fetch / Reader
Fetch extracts readable text from a URL and stores it as artifacts.

Behavior:
- One request creates one task of type `fetch_url`.
- Clean text is stored in a `fetch_text` artifact.
- Raw HTML is stored as `fetch_html` only when `store_raw_html=true`.
- Cache key is the canonicalized URL; identical fetches reuse artifacts.

Endpoint:
```bash
POST /fetch
```

Request:
```json
{
  "url": "https://example.com/article",
  "reader_mode": true,
  "store_raw_html": true
}
```
