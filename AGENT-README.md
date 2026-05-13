# Agent README

This document is for agents operating on or against `gregs-api` in `/home/adminuser/api`.

It is not product copy. It is the working description of how this system is laid out, how to use it safely, and where the current sharp edges are.

## What This Repo Is

`gregs-api` is a FastAPI service with:
- PostgreSQL persistence
- Redis-backed durable task queue
- MinIO artifact storage
- optional Qdrant-backed knowledge promotion/query
- a distributed Hermes coordination board

This repo now has two distinct work systems:

1. Durable async task runner:
   - root endpoints like `/tasks`, `/search`, `/fetch`, `/import/*`, `/knowledge/*`
   - Redis queue + worker process
   - results usually land in artifacts

2. Coordination board:
   - `/api/board/*`
   - Postgres-backed coordination cards for humans and remote Hermes agents
   - not a Redis queue replacement

Do not confuse them.

## Source Of Truth

Main entrypoints:
- [app/main.py](/home/adminuser/api/app/main.py:1)
- [app/models.py](/home/adminuser/api/app/models.py:1)
- [app/artifacts.py](/home/adminuser/api/app/artifacts.py:1)
- [app/board/router.py](/home/adminuser/api/app/board/router.py:1)
- [app/board/service.py](/home/adminuser/api/app/board/service.py:1)
- [worker/worker.py](/home/adminuser/api/worker/worker.py:1)
- [board_worker/runner.py](/home/adminuser/api/board_worker/runner.py:1)

If docs and code disagree, trust code.

## Repo Layout

- `app/main.py`: main FastAPI app, root task/search/fetch/import/knowledge endpoints, health, limits
- `app/models.py`: SQLAlchemy models, including board tables
- `app/db.py`: DB session setup and migration helper
- `app/queue.py`: Redis queue helpers for the durable task system
- `app/search.py`: Brave / Exa search task logic
- `app/fetch.py`: fetch task logic and reader-mode extraction
- `app/imports.py`: file and ChatGPT import helpers
- `app/knowledge/`: knowledge promotion/query plumbing
- `app/artifacts.py`: artifact APIs and artifact persistence helpers
- `app/board/`: board constants, auth, schemas, router, service
- `worker/worker.py`: Redis task worker
- `board_worker/`: remote Hermes board worker CLI
- `scripts/`: smoke tests
- `alembic/versions/`: migrations

## Runtime Model

### Core API

The API container listens internally on port `8000`.

Common local access patterns:
- direct local API: `http://127.0.0.1:8000`
- nginx frontend proxy: `/api/...`

### Durable Task Runner

The root task system is for queued execution.

Typical flow:
1. POST a task request
2. worker pulls from Redis
3. worker executes
4. status updates are persisted in Postgres
5. outputs are written as artifacts

### Board

The board is for human/agent coordination.

Typical flow:
1. operator creates a board task
2. task moves to `ready`
3. remote worker claims it
4. worker starts, comments, adds artifacts
5. worker completes / blocks / fails
6. event log preserves audit history

Board state is Postgres-driven, not Redis-driven.

## Important Distinction: `/tasks` vs `/api/board/tasks`

`/tasks`
- Redis-backed durable async jobs
- machine execution
- task types like `echo`, `search`, `fetch`, import, knowledge

`/api/board/tasks`
- coordination cards
- human and Hermes workflow tracking
- claim / heartbeat / comment / artifact / lifecycle endpoints

Do not route board work through `/tasks` unless you intentionally want a queued root-task job.

## API Surfaces

### Health And Limits

- `GET /health`
- `GET /limits`

Use these first when onboarding or diagnosing.

### Durable Task System

- `POST /tasks`
- `GET /tasks`
- `GET /tasks/{task_id}`
- `POST /tasks/{task_id}/cancel`

Search / fetch / imports / knowledge all enqueue work into the durable task system:

- `POST /search`
- `POST /search/batch`
- `POST /fetch`
- `POST /import/files`
- `POST /import/chatgpt`
- `POST /knowledge/promote`
- `POST /knowledge/query`

### Artifacts

- `POST /artifacts`
- `GET /artifacts`
- `GET /artifacts/{artifact_id}`
- `GET /artifacts/{artifact_id}/meta`
- `GET /tasks/{task_id}/artifacts`

Artifacts are a primary contract in this repo. Prefer inspecting artifacts over guessing what a worker produced.

### Board

Agents:
- `POST /api/board/agents/register`
- `POST /api/board/agents/heartbeat`
- `GET /api/board/agents`

Tasks:
- `POST /api/board/tasks`
- `GET /api/board/tasks`
- `GET /api/board/tasks/{task_id}`
- `PATCH /api/board/tasks/{task_id}`

Claim / lifecycle:
- `POST /api/board/tasks/{task_id}/claim`
- `POST /api/board/tasks/{task_id}/heartbeat`
- `POST /api/board/tasks/{task_id}/release`
- `POST /api/board/tasks/{task_id}/start`
- `POST /api/board/tasks/{task_id}/complete`
- `POST /api/board/tasks/{task_id}/block`
- `POST /api/board/tasks/{task_id}/fail`
- `POST /api/board/tasks/{task_id}/cancel`

Comments / artifacts / events:
- `GET /api/board/tasks/{task_id}/comments`
- `POST /api/board/tasks/{task_id}/comments`
- `GET /api/board/tasks/{task_id}/artifacts`
- `POST /api/board/tasks/{task_id}/artifacts`
- `GET /api/board/events`

## Board Data Model

Board tables:
- `board_agents`
- `board_tasks`
- `board_comments`
- `board_events`
- `board_task_artifacts`

Allowed task statuses:
- `triage`
- `todo`
- `ready`
- `claimed`
- `running`
- `blocked`
- `failed`
- `done`
- `cancelled`

Allowed comment types:
- `info`
- `status`
- `blocker`
- `system`
- `artifact`
- `escalation`

## Board Auth

Board auth is optional.

If neither of these is set, board endpoints are open:
- `BOARD_ADMIN_TOKEN`
- `BOARD_WORKER_TOKENS_JSON`

If either is set, board endpoints require `Authorization: Bearer <token>`.

### Admin token

Admin can do everything board-related.

Current local dev convention in this environment has been:
- `board-admin-dev`

Do not assume that value outside the local machine.

### Worker tokens

Worker tokens encode:
- `agent_name`
- `host_name`
- `allowed_capabilities`

Workers may:
- register and heartbeat themselves
- patch their own stored agent profile
- claim allowed `ready` tasks
- act only as themselves
- comment and attach artifacts on any visible non-cancelled task
- create child tasks under shared parent tasks they participate in
- move their own unclaimed child tasks from `triage` or `todo` to `ready`
- release their own stale claim
- complete/block/fail/release/start/heartbeat only on tasks they currently claim

Workers may not:
- patch arbitrary root-task fields
- cancel tasks
- delete tasks
- impersonate another worker

## Board Operational Rules

Hard server-side limits:
- `BOARD_MAX_COMMENTS_PER_TASK`
- `BOARD_MAX_CHILD_TASKS`
- `BOARD_MAX_REASSIGNMENTS`
- `BOARD_CLAIM_TTL_SECONDS`
- `BOARD_AGENT_OFFLINE_AFTER_SECONDS`
- `BOARD_MAX_TASK_BODY_CHARS`
- `BOARD_MAX_COMMENT_CHARS`

Current defaults are exposed by `GET /limits`.

Important behavior:
- claim is DB-atomic
- claim only works for `ready` or expired claims
- board uses row locking in Postgres
- claim is an execution lock, not a collaboration lock
- comment and child-task overflows can auto-block tasks
- some internal counters are stored in metadata under `_system` and filtered out of API responses
- failed permissioned mutations return diagnostic denial details and emit `board_authorization_denied` events

## Board Worker

Entry point:
- [board_worker/runner.py](/home/adminuser/api/board_worker/runner.py:1)

Run it with:

```bash
export BOARD_WORKER_RICK_TOKEN=rick-token
python -m board_worker.runner --config board-worker-rick.yaml
```

```bash
export BOARD_WORKER_GREG_TOKEN=greg-token
python -m board_worker.runner --config board-worker-greg.yaml
```

Config example:
- [board-worker.example.yaml](/home/adminuser/api/board-worker.example.yaml:1)
- [board-worker-rick.yaml](/home/adminuser/api/board-worker-rick.yaml:1)
- [board-worker-greg.yaml](/home/adminuser/api/board-worker-greg.yaml:1)

Worker loop:
1. register agent
2. poll `ready` tasks
3. claim
4. start
5. load comments
6. invoke local Hermes subprocess
7. post result comment / artifact
8. complete / block / fail

Hermes invocation is subprocess-based for now.

Cron/non-interactive parity:
- board workers send the same bearer token as interactive workers
- optional Cloudflare Access headers are supported:
  - `CF-Access-Client-Id`
  - `CF-Access-Client-Secret`
- worker config keys:
  - `api_url`
  - `api_token`
  - `canonical_agent_name`
  - `cf_access_client_id`
  - `cf_access_client_secret`

## Current Frontend / Proxy Quirk

This matters if an agent is testing through the nginx-served `/ops` frontend rather than talking directly to the API.

Current nginx behavior strips the `/api` prefix before proxying to FastAPI. Root APIs are compatible with that, but board routes are mounted at `/api/board/...` inside FastAPI.

As a result:
- direct API path: `http://127.0.0.1:8000/api/board/...`
- nginx frontend path currently used by the board UI bundle: `/api/api/board/...`

This is a deployment quirk, not an intended public contract.

If you are scripting against the API directly, prefer the direct FastAPI path:
- `http://127.0.0.1:8000/api/board/...`

## Knowledge System

Knowledge features are isolated under:
- `app/knowledge/`
- root endpoints in `app/main.py`

Current intent:
- ingest artifacts
- optionally promote them into domain-scoped Qdrant collections
- query those collections asynchronously through the durable task system

Knowledge domains currently used in the operator frontend:
- `project`
- `ops`
- `reference`
- `archive`
- `influence`
- `cognition`
- `belief`

Do not modify knowledge behavior casually. It is an existing subsystem and was an explicit non-regression constraint during board work.

## Artifacts Matter

This system is artifact-first.

That means:
- worker outputs are often preserved as artifacts
- imports stage source files as artifacts
- board task artifacts should prefer linking to the existing artifact system when possible

When debugging or validating behavior:
1. inspect task status
2. inspect linked artifacts
3. inspect board comments/events if board-related

## Local Runbook

Start stack:

```bash
cd /home/adminuser/api
make start
```

Run migrations:

```bash
make init-db
```

Check health:

```bash
docker compose exec -T api curl -s http://127.0.0.1:8000/health
```

Check limits:

```bash
docker compose exec -T api curl -s http://127.0.0.1:8000/limits
```

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

If board auth is enabled:

```bash
BOARD_AUTH_TOKEN="${BOARD_ADMIN_TOKEN}" make smoke-board
```

## Minimal API Examples

### Create a durable task

```bash
curl -s -X POST http://127.0.0.1:8000/tasks \
  -H 'Content-Type: application/json' \
  -d '{"type":"echo","params":{"msg":"hello"},"timeout_seconds":300,"max_retries":0}'
```

### Inspect a durable task

```bash
curl -s http://127.0.0.1:8000/tasks/<task_id>
```

### Create a board task

```bash
curl -s -X POST http://127.0.0.1:8000/api/board/tasks \
  -H 'Authorization: Bearer <admin-token>' \
  -H 'Content-Type: application/json' \
  -d '{
    "title":"Example board task",
    "body":"Coordinate a remote Hermes action.",
    "status":"ready",
    "priority":2,
    "created_by":"operator",
    "requested_capability":"coding",
    "allowed_capabilities":["coding","git"],
    "watchers":["Greg","Rick"],
    "contributors":["Greg","Rick"]
  }'
```

### Claim a board task

```bash
curl -s -X POST http://127.0.0.1:8000/api/board/tasks/<task_id>/claim \
  -H 'Authorization: Bearer <worker-token>' \
  -H 'Content-Type: application/json' \
  -d '{"agent_name":"Rick","claim_ttl_seconds":120}'
```

### Complete a board task

```bash
curl -s -X POST http://127.0.0.1:8000/api/board/tasks/<task_id>/complete \
  -H 'Authorization: Bearer <worker-token>' \
  -H 'Content-Type: application/json' \
  -d '{"agent_name":"Rick","metadata":{"result":"done"}}'
```

## Onboarding Checklist For Agents

1. Read [README.md](/home/adminuser/api/README.md:1)
2. Read this file
3. Check [app/main.py](/home/adminuser/api/app/main.py:1) for current mounted routes
4. Check [app/board/router.py](/home/adminuser/api/app/board/router.py:1) if touching board behavior
5. Check [app/limits.py](/home/adminuser/api/app/limits.py:1) before assuming payload sizes or loop ceilings
6. Run `GET /health`
7. Run `GET /limits`
8. Use smoke scripts before and after meaningful changes

## Safety Notes

- Do not conflate board cards with durable root tasks.
- Do not modify knowledge functionality unless explicitly asked.
- Prefer additive changes over changing existing task runner contracts.
- Do not assume nginx path behavior matches direct FastAPI routing.
- When the board is authenticated, test both authenticated and unauthenticated behavior.
- Prefer verifying through artifacts and persisted events instead of relying on console output alone.
