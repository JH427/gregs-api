#!/usr/bin/env bash
set -euo pipefail

API_BASE="http://127.0.0.1:8000"

run_in_api() {
  docker compose exec -T api "$@"
}

wait_for_task() {
  local task_id="$1"
  local expected="$2"
  for _ in $(seq 1 60); do
    STATUS=$(run_in_api curl -s "$API_BASE/tasks/$task_id" | python3 -c 'import sys, json; print(json.load(sys.stdin)["status"])')
    if [ "$STATUS" = "$expected" ]; then
      return 0
    fi
    sleep 1
  done
  echo "task $task_id did not reach status $expected in time" >&2
  exit 1
}

TMP_DIR=$(mktemp -d)
trap 'rm -rf "$TMP_DIR"' EXIT

TEXT_PATH="$TMP_DIR/knowledge.txt"
CONTAINER_TEXT_PATH="/tmp/knowledge-smoke.txt"

cat > "$TEXT_PATH" <<'EOF'
Narsil promotion smoke test.
This text should be chunked deterministically and stored in knowledge memory.
EOF

docker compose exec -T api curl -s "$API_BASE/health" >/dev/null
docker compose cp "$TEXT_PATH" "api:$CONTAINER_TEXT_PATH" >/dev/null

IMPORT_TASK_ID=$(run_in_api curl -s -X POST "$API_BASE/import/files" \
  -F "file=@$CONTAINER_TEXT_PATH;type=text/plain" \
  | python3 -c 'import sys, json; print(json.load(sys.stdin)["task_id"])')

wait_for_task "$IMPORT_TASK_ID" "completed"

SOURCE_ARTIFACT_ID=$(run_in_api curl -s "$API_BASE/tasks/$IMPORT_TASK_ID/artifacts" | python3 -c '
import sys, json
payload = json.load(sys.stdin)
for artifact in payload["artifacts"]:
    if artifact["type"] == "import_parsed_text":
        print(artifact["artifact_id"])
        break
else:
    raise SystemExit("no import_parsed_text artifact found")
')

PROMOTE_TASK_ID=$(run_in_api curl -s -X POST "$API_BASE/knowledge/promote" \
  -H 'Content-Type: application/json' \
  -d "{\"artifact_id\":\"$SOURCE_ARTIFACT_ID\",\"domain\":\"project\",\"source\":\"files\",\"confidence\":\"high\"}" \
  | python3 -c 'import sys, json; print(json.load(sys.stdin)["task_id"])')

wait_for_task "$PROMOTE_TASK_ID" "completed"

run_in_api curl -s "$API_BASE/tasks/$PROMOTE_TASK_ID" | python3 -c '
import sys, json
payload = json.load(sys.stdin)
assert "Promoted artifact" in payload["result"]["summary"], payload
print(payload)
'

run_in_api curl -s "$API_BASE/tasks/$PROMOTE_TASK_ID/artifacts" | python3 -c '
import sys, json
payload = json.load(sys.stdin)
types = {item["type"] for item in payload["artifacts"]}
assert "knowledge_promotion_report" in types, payload
assert "knowledge_document_chunks" in types, payload
print(payload)
'

DEDUP_TASK_ID=$(run_in_api curl -s -X POST "$API_BASE/knowledge/promote" \
  -H 'Content-Type: application/json' \
  -d "{\"artifact_id\":\"$SOURCE_ARTIFACT_ID\",\"domain\":\"project\",\"source\":\"files\",\"confidence\":\"high\"}" \
  | python3 -c 'import sys, json; print(json.load(sys.stdin)["task_id"])')

wait_for_task "$DEDUP_TASK_ID" "completed"

REPORT_ID=$(run_in_api curl -s "$API_BASE/tasks/$DEDUP_TASK_ID" | python3 -c '
import sys, json
payload = json.load(sys.stdin)
assert "deduped" in payload["result"]["summary"].lower(), payload
print(payload["result"]["artifact_ids"][0])
')

run_in_api curl -s "$API_BASE/artifacts/$REPORT_ID" | python3 -c '
import sys, json
payload = json.load(sys.stdin)
assert payload["deduped"] is True, payload
print(payload)
'

BELIEF_STATUS=$(run_in_api sh -lc "curl -s -o /tmp/knowledge-belief.json -w '%{http_code}' -X POST $API_BASE/knowledge/promote -H 'Content-Type: application/json' -d '{\"artifact_id\":\"$SOURCE_ARTIFACT_ID\",\"domain\":\"belief\"}'")
if [ "$BELIEF_STATUS" != "403" ]; then
  echo "expected BELIEF promotion to return 403, got $BELIEF_STATUS" >&2
  exit 1
fi
