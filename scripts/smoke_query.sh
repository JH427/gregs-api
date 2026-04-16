#!/usr/bin/env bash
set -euo pipefail

API_BASE="http://127.0.0.1:8000"

run_in_api() {
  docker compose exec -T api "$@"
}

wait_for_task() {
  local task_id="$1"
  local expected="$2"
  for _ in $(seq 1 90); do
    STATUS=$(run_in_api curl -s "$API_BASE/tasks/$task_id" | python3 -c 'import sys, json; print(json.load(sys.stdin)["status"])')
    if [ "$STATUS" = "$expected" ]; then
      return 0
    fi
    sleep 1
  done
  echo "task $task_id did not reach status $expected in time" >&2
  exit 1
}

create_import_and_get_parsed_artifact() {
  local host_path="$1"
  local container_path="$2"
  docker compose cp "$host_path" "api:$container_path" >/dev/null
  local task_id
  task_id=$(run_in_api curl -s -X POST "$API_BASE/import/files" \
    -F "file=@$container_path;type=text/plain" \
    | python3 -c 'import sys, json; print(json.load(sys.stdin)["task_id"])')
  wait_for_task "$task_id" "completed"
  run_in_api curl -s "$API_BASE/tasks/$task_id/artifacts" | python3 -c '
import sys, json
payload = json.load(sys.stdin)
for artifact in payload["artifacts"]:
    if artifact["type"] == "import_parsed_text":
        print(artifact["artifact_id"])
        break
else:
    raise SystemExit("no import_parsed_text artifact found")
'
}

promote_artifact() {
  local artifact_id="$1"
  local domain="$2"
  local source="$3"
  local confidence="$4"
  local task_id
  task_id=$(run_in_api curl -s -X POST "$API_BASE/knowledge/promote" \
    -H 'Content-Type: application/json' \
    -d "{\"artifact_id\":\"$artifact_id\",\"domain\":\"$domain\",\"source\":\"$source\",\"confidence\":\"$confidence\"}" \
    | python3 -c 'import sys, json; print(json.load(sys.stdin)["task_id"])')
  wait_for_task "$task_id" "completed"
}

TMP_DIR=$(mktemp -d)
trap 'rm -rf "$TMP_DIR"' EXIT

PROJECT_PATH="$TMP_DIR/project.txt"
OPS_PATH="$TMP_DIR/ops.txt"

cat > "$PROJECT_PATH" <<'EOF'
Project decision log.
The batch search endpoint was implemented to support multiple Brave queries in a single task.
The project decision was to keep results artifact-first and return only artifact references.
EOF

cat > "$OPS_PATH" <<'EOF'
Ops incident note.
Brave batch was failing because redis queue pressure and rate limit handling needed inspection.
Docker, Redis, Qdrant, and Uvicorn operational behavior are tracked in ops knowledge.
EOF

docker compose exec -T api curl -s "$API_BASE/health" >/dev/null

PROJECT_ARTIFACT_ID=$(create_import_and_get_parsed_artifact "$PROJECT_PATH" "/tmp/query-project.txt")
OPS_ARTIFACT_ID=$(create_import_and_get_parsed_artifact "$OPS_PATH" "/tmp/query-ops.txt")

promote_artifact "$PROJECT_ARTIFACT_ID" "project" "manual" "high"
promote_artifact "$OPS_ARTIFACT_ID" "ops" "manual" "medium"

ONLY_TASK_ID=$(run_in_api curl -s -X POST "$API_BASE/knowledge/query" \
  -H 'Content-Type: application/json' \
  -d '{"query":"artifact references decision","domains":["project"],"domain_mode":"only","top_k_per_domain":5}' \
  | python3 -c 'import sys, json; print(json.load(sys.stdin)["task_id"])')
wait_for_task "$ONLY_TASK_ID" "completed"

ONLY_RESULTS_ID=$(run_in_api curl -s "$API_BASE/tasks/$ONLY_TASK_ID" | python3 -c '
import sys, json
payload = json.load(sys.stdin)
assert payload["result"]["artifact_ids"], payload
print(payload["result"]["artifact_ids"][0])
')
ONLY_AUDIT_ID=$(run_in_api curl -s "$API_BASE/tasks/$ONLY_TASK_ID" | python3 -c '
import sys, json
payload = json.load(sys.stdin)
print(payload["result"]["artifact_ids"][1])
')
run_in_api curl -s "$API_BASE/artifacts/$ONLY_RESULTS_ID" | python3 -c '
import sys, json
payload = json.load(sys.stdin)
assert payload["results"], payload
print(payload)
'
run_in_api curl -s "$API_BASE/artifacts/$ONLY_AUDIT_ID" | python3 -c '
import sys, json
payload = json.load(sys.stdin)
assert payload["domains_searched"] == ["project"], payload
print(payload)
'

PREFER_TASK_ID=$(run_in_api curl -s -X POST "$API_BASE/knowledge/query" \
  -H 'Content-Type: application/json' \
  -d '{"query":"redis queue pressure","domains":["ops"],"domain_mode":"prefer","top_k_per_domain":5}' \
  | python3 -c 'import sys, json; print(json.load(sys.stdin)["task_id"])')
wait_for_task "$PREFER_TASK_ID" "completed"
PREFER_AUDIT_ID=$(run_in_api curl -s "$API_BASE/tasks/$PREFER_TASK_ID" | python3 -c '
import sys, json
payload = json.load(sys.stdin)
print(payload["result"]["artifact_ids"][1])
')
run_in_api curl -s "$API_BASE/artifacts/$PREFER_AUDIT_ID" | python3 -c '
import sys, json
payload = json.load(sys.stdin)
assert "ops" in payload["domains_searched"], payload
assert payload["domain_mode"] == "prefer", payload
print(payload)
'

ROUTER_TASK_ID=$(run_in_api curl -s -X POST "$API_BASE/knowledge/query" \
  -H 'Content-Type: application/json' \
  -d '{"query":"docker redis rate limit decision","top_k_per_domain":5}' \
  | python3 -c 'import sys, json; print(json.load(sys.stdin)["task_id"])')
wait_for_task "$ROUTER_TASK_ID" "completed"
ROUTER_RESULTS_ID=$(run_in_api curl -s "$API_BASE/tasks/$ROUTER_TASK_ID" | python3 -c '
import sys, json
payload = json.load(sys.stdin)
print(payload["result"]["artifact_ids"][0])
')
ROUTER_AUDIT_ID=$(run_in_api curl -s "$API_BASE/tasks/$ROUTER_TASK_ID" | python3 -c '
import sys, json
payload = json.load(sys.stdin)
print(payload["result"]["artifact_ids"][1])
')
run_in_api curl -s "$API_BASE/artifacts/$ROUTER_RESULTS_ID" | python3 -c '
import sys, json
payload = json.load(sys.stdin)
assert payload["results"], payload
print(payload)
'
run_in_api curl -s "$API_BASE/artifacts/$ROUTER_AUDIT_ID" | python3 -c '
import sys, json
payload = json.load(sys.stdin)
assert payload["routing"]["used_router"] is True, payload
assert "ops" in payload["domains_searched"], payload
print(payload)
'

ZERO_TASK_ID=$(run_in_api curl -s -X POST "$API_BASE/knowledge/query" \
  -H 'Content-Type: application/json' \
  -d '{"query":"docker redis rate limit decision","domains":["ops","project"],"domain_mode":"only","filters":{"source":["email"]},"top_k_per_domain":5}' \
  | python3 -c 'import sys, json; print(json.load(sys.stdin)["task_id"])')
wait_for_task "$ZERO_TASK_ID" "completed"
run_in_api curl -s "$API_BASE/tasks/$ZERO_TASK_ID" | python3 -c '
import sys, json
payload = json.load(sys.stdin)
assert "Returned 0 results" in payload["result"]["summary"], payload
print(payload)
'
ZERO_RESULTS_ID=$(run_in_api curl -s "$API_BASE/tasks/$ZERO_TASK_ID" | python3 -c '
import sys, json
payload = json.load(sys.stdin)
print(payload["result"]["artifact_ids"][0])
')
ZERO_AUDIT_ID=$(run_in_api curl -s "$API_BASE/tasks/$ZERO_TASK_ID" | python3 -c '
import sys, json
payload = json.load(sys.stdin)
print(payload["result"]["artifact_ids"][1])
')
run_in_api curl -s "$API_BASE/artifacts/$ZERO_RESULTS_ID" | python3 -c '
import sys, json
payload = json.load(sys.stdin)
assert payload["results"] == [], payload
print(payload)
'
run_in_api curl -s "$API_BASE/artifacts/$ZERO_AUDIT_ID" | python3 -c '
import sys, json
payload = json.load(sys.stdin)
assert payload["filters_applied"]["source"] == ["email"], payload
print(payload)
'
