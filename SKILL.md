# SKILL: FullyAutomated Task API (Narsil)

This skill teaches Greg how to use the internal Task Runner + Queue API hosted at `https://api.fullyautomated.dev`. The API is protected by Cloudflare Access; all requests must be made from an authorized environment.

## What This API Does
- **Asynchronous tasks** with a stable contract (`/tasks`, `/tasks/{id}`, `/tasks/{id}/cancel`, `/health`).
- **Search** is exposed via `/search`, but still executes as a queued task.
- **Batch search** is exposed via `/search/batch` and enqueues a single task for many queries.
- **Fetch** is exposed via `/fetch` and extracts readable text from a URL.
- **Artifacts** store large outputs in MinIO and are fetched via `/artifacts/{id}`.
- **Deduplication** is automatic in the worker based on normalized params. Duplicate tasks converge to prior results.

## Critical Constraints (Do Not Violate)
- Do not assume direct access without Cloudflare Access.
- Do not expect synchronous results from `/tasks` or `/search`.
- Do not send large blobs in task params. Use artifacts.
- Do not add auth logic client‑side; Access already protects the origin.

## Base URL
- `https://api.fullyautomated.dev`

## Authentication
- Cloudflare Access sits in front of the API. Use a session/token that has Access approved. If Access is not satisfied, requests will be blocked.

## Task Lifecycle
1. **Create task** (enqueue): `POST /tasks`, `POST /search`, or `POST /search/batch`
2. **Poll**: `GET /tasks/{id}` until status is `completed`, `failed`, or `cancelled`
3. **Fetch artifacts** if returned in `result.artifact_ids`

## Task API
### POST /tasks
Enqueue an asynchronous task.

Request:
```json
{
  "type": "echo" | "sleep" | "search",
  "params": { "...": "..." },
  "idempotency_key": "optional",
  "timeout_seconds": 300,
  "max_retries": 0
}
```

Response (202):
```json
{ "task_id": "...", "status": "queued" }
```

### GET /tasks/{id}
Check status and result.

Response:
```json
{
  "task_id": "...",
  "status": "queued|running|completed|failed|cancelled",
  "type": "...",
  "created_at": "...",
  "updated_at": "...",
  "started_at": "...",
  "finished_at": "...",
  "result": {
    "artifact_ids": ["..."],
    "summary": "..."
  },
  "error": "..." | null
}
```

### POST /tasks/{id}/cancel
Best‑effort cancellation. Returns immediately.

Response:
```json
{ "task_id": "...", "status": "cancel_requested" | "cancelled" }
```

### GET /health
```json
{ "ok": true, "redis": "ok|down", "db": "ok|down", "worker": "ok|unknown" }
```

## Search API
### POST /search
Enqueues a search task (never synchronous).

Request:
```json
{
  "query": "string",
  "sources": ["brave"],
  "recency_days": 7
}
```

Response (202):
```json
{ "task_id": "...", "status": "queued" }
```

Poll via `GET /tasks/{id}`. Normalized results are stored in an artifact with type `search_results`. Raw provider responses are stored as `search_raw` artifacts.

Providers:
- `brave` (default)
- `exa` (requires `EXA_API_KEY` and allowlist entry)

When to choose:
- Use **exa** for high-signal research and cleaner results.
- Use **brave** for broad recon and coverage.

### POST /search/batch
Enqueues a batch search task (never synchronous). One task handles multiple queries.

Constraints:
- `queries` length must be between 1 and `MAX_BATCH_SIZE` (default 50).
- `sources` must be allowed (default allowlist: `brave` only).

Request:
```json
{
  "queries": ["query one", "query two"],
  "sources": ["brave"],
  "recency_days": 7
}
```

Response (202):
```json
{ "task_id": "...", "status": "queued" }
```

Poll via `GET /tasks/{id}`. Grouped results are stored in an artifact with type `search_batch_results`. Raw provider responses are stored as `search_raw` artifacts.

Batch caching:
- Each query uses the same cache key as single-search.
- Overlapping batches reuse cached results.
- Identical batches dedupe to prior artifacts.

Exa notes:
- If you request `exa` and the key is missing, the task is rejected deterministically.
- `recency_days` is mapped to a published-date filter on Exa (best-effort).

## Fetch API
### POST /fetch
Fetch a URL and extract readable text (never synchronous).

Request:
```json
{
  "url": "https://example.com/article",
  "reader_mode": true,
  "store_raw_html": true
}
```

Response (202):
```json
{ "task_id": "...", "status": "queued" }
```

Notes:
- Domain allowlist is enforced via `FETCH_DOMAIN_ALLOWLIST`.
- Results are stored as `fetch_text` (and `fetch_html` if requested).

## Artifacts
### GET /artifacts/{id}
Fetches artifact content (streamed).

### GET /artifacts/{id}/meta
Returns metadata:
```json
{
  "artifact_id": "...",
  "task_id": "...",
  "type": "search_results|search_raw|echo_result|sleep_result|...",
  "content_type": "...",
  "created_at": "..."
}
```

## Deduplication Behavior (Important)
- The worker computes a **processing key** as: `task.type + hash(normalized_params)`.
- If a completed run exists for that key, the worker **skips re‑execution** and marks the new task `completed`.
- The new task’s `result.artifact_ids` points to the prior artifacts.
- This is automatic. Greg can safely retry identical work and get fast convergence.

## Usage Patterns
### 1) Submit a task, poll until completion
```bash
curl -s -X POST https://api.fullyautomated.dev/tasks \
  -H 'Content-Type: application/json' \
  -d '{"type":"echo","params":{"msg":"hello"},"timeout_seconds":30,"max_retries":0}'

curl -s https://api.fullyautomated.dev/tasks/<task_id>
```

### 2) Submit a search, fetch normalized results artifact
```bash
curl -s -X POST https://api.fullyautomated.dev/search \
  -H 'Content-Type: application/json' \
  -d '{"query":"open source task runner","sources":["brave"],"recency_days":7}'

curl -s https://api.fullyautomated.dev/tasks/<task_id>
# -> result.artifact_ids includes search_results

curl -s https://api.fullyautomated.dev/artifacts/<artifact_id>
```

### 2b) Submit a batch search, fetch grouped results artifact
```bash
curl -s -X POST https://api.fullyautomated.dev/search/batch \
  -H 'Content-Type: application/json' \
  -d '{"queries":["queue worker redis","fastapi task runner"],"sources":["brave"],"recency_days":7}'

curl -s https://api.fullyautomated.dev/tasks/<task_id>
# -> result.artifact_ids includes search_batch_results

curl -s https://api.fullyautomated.dev/artifacts/<artifact_id>
```

### 2c) Fetch a URL (reader mode)
```bash
curl -s -X POST https://api.fullyautomated.dev/fetch \
  -H 'Content-Type: application/json' \
  -d '{"url":"https://example.com/article","reader_mode":true,"store_raw_html":true}'

curl -s https://api.fullyautomated.dev/tasks/<task_id>
# -> result.artifact_ids includes fetch_text (and fetch_html if requested)

curl -s https://api.fullyautomated.dev/artifacts/<artifact_id>
```

### 3) Cancel a long task
```bash
curl -s -X POST https://api.fullyautomated.dev/tasks/<task_id>/cancel
```

## Error Handling Rules
- If task status is `failed`, read `error` and do not assume artifacts exist.
- If task status is `cancelled`, do not expect artifacts.
- If artifact fetch returns 404, it is permanently missing; handle gracefully.

## Best Practices for Greg
- Always treat task execution as async.
- Prefer idempotency or identical params for convergence.
- Store large outputs as artifacts and only return small summaries in results.
- Poll with backoff to avoid hammering the API.

## Quick Checklist
- Access approved? ✅
- Task enqueued? ✅
- Poll until `completed`? ✅
- Fetch artifacts if present? ✅
- Handle `failed` or `cancelled`? ✅

End of skill.
