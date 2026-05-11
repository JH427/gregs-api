#!/usr/bin/env bash
set -euo pipefail

API_BASE="${BOARD_API_BASE:-http://127.0.0.1:8000/api/board}"
AUTH_HEADER=()

if [ -n "${BOARD_AUTH_TOKEN:-}" ]; then
  AUTH_HEADER=(-H "Authorization: Bearer $BOARD_AUTH_TOKEN")
fi

run_in_api() {
  docker compose exec -T api "$@"
}

AGENT_JSON=$(run_in_api curl -s -X POST "$API_BASE/agents/register" \
  -H 'Content-Type: application/json' \
  "${AUTH_HEADER[@]}" \
  -d '{"name":"Rick","host":"titan","capabilities":["coordination"],"status":"idle","metadata":{"smoke":true}}')
AGENT_ID=$(printf '%s' "$AGENT_JSON" | python3 -c 'import sys, json; print(json.load(sys.stdin)["id"])')
echo "agent_id=$AGENT_ID"

TASK_JSON=$(run_in_api curl -s -X POST "$API_BASE/tasks" \
  -H 'Content-Type: application/json' \
  "${AUTH_HEADER[@]}" \
  -d '{"title":"Smoke board task","body":"Verify board lifecycle.","status":"ready","priority":1,"requested_capability":"coordination","created_by":"smoke"}')
TASK_ID=$(printf '%s' "$TASK_JSON" | python3 -c 'import sys, json; print(json.load(sys.stdin)["id"])')
echo "task_id=$TASK_ID"

run_in_api curl -s -X POST "$API_BASE/tasks/$TASK_ID/claim" \
  -H 'Content-Type: application/json' \
  "${AUTH_HEADER[@]}" \
  -d '{"agent_name":"Rick","claim_ttl_seconds":120}' \
  | python3 -c 'import sys, json; payload=json.load(sys.stdin); assert payload["status"]=="claimed", payload; print(payload)'

run_in_api curl -s -X POST "$API_BASE/tasks/$TASK_ID/heartbeat" \
  -H 'Content-Type: application/json' \
  "${AUTH_HEADER[@]}" \
  -d '{"agent_name":"Rick","claim_ttl_seconds":120}' \
  | python3 -c 'import sys, json; payload=json.load(sys.stdin); assert payload["claimed_by"]=="Rick", payload; print(payload)'

run_in_api curl -s -X POST "$API_BASE/tasks/$TASK_ID/comments" \
  -H 'Content-Type: application/json' \
  "${AUTH_HEADER[@]}" \
  -d '{"author":"Rick","comment_type":"status","body":"smoke comment"}' \
  | python3 -c 'import sys, json; payload=json.load(sys.stdin); assert payload["comment_type"]=="status", payload; print(payload)'

run_in_api curl -s -X POST "$API_BASE/tasks/$TASK_ID/complete" \
  -H 'Content-Type: application/json' \
  "${AUTH_HEADER[@]}" \
  -d '{"agent_name":"Rick","metadata":{"result":"ok"}}' \
  | python3 -c 'import sys, json; payload=json.load(sys.stdin); assert payload["status"]=="done", payload; print(payload)'

run_in_api curl -s "${AUTH_HEADER[@]}" "$API_BASE/events?task_id=$TASK_ID&limit=20" | python3 -c '
import sys, json
payload = json.load(sys.stdin)
event_types = [item["event_type"] for item in payload["events"]]
required = {"board_task_created", "board_task_claimed", "board_task_heartbeat", "board_comment_created", "board_task_completed"}
assert required.issubset(set(event_types)), payload
print(payload)
'

run_in_api curl -s "${AUTH_HEADER[@]}" "$API_BASE/tasks/$TASK_ID" | python3 -c '
import sys, json
payload = json.load(sys.stdin)
assert payload["status"] == "done", payload
print(payload)
'
