#!/usr/bin/env bash
set -euo pipefail

API_BASE="http://127.0.0.1:8000"

run_in_api() {
  docker compose exec -T api "$@"
}

# Echo task
TASK_ID=$(run_in_api curl -s -X POST "$API_BASE/tasks" \
  -H 'Content-Type: application/json' \
  -d '{"type":"echo","params":{"msg":"hello"},"timeout_seconds":30,"max_retries":0}' \
  | python3 -c 'import sys, json; print(json.load(sys.stdin)["task_id"])')

echo "echo task_id=$TASK_ID"

for i in $(seq 1 20); do
  STATUS=$(run_in_api curl -s "$API_BASE/tasks/$TASK_ID" | python3 -c 'import sys, json; print(json.load(sys.stdin)["status"])')
  if [ "$STATUS" = "completed" ]; then
    run_in_api curl -s "$API_BASE/tasks/$TASK_ID" | python3 -c 'import sys, json; print(json.load(sys.stdin))'
    break
  fi
  sleep 1
  if [ "$i" -eq 20 ]; then
    echo "echo task did not complete in time" >&2
    exit 1
  fi
done

# Sleep and cancel
SLEEP_ID=$(run_in_api curl -s -X POST "$API_BASE/tasks" \
  -H 'Content-Type: application/json' \
  -d '{"type":"sleep","params":{"seconds":10},"timeout_seconds":30,"max_retries":0}' \
  | python3 -c 'import sys, json; print(json.load(sys.stdin)["task_id"])')

echo "sleep task_id=$SLEEP_ID"

run_in_api curl -s -X POST "$API_BASE/tasks/$SLEEP_ID/cancel" | python3 -c 'import sys, json; print(json.load(sys.stdin))'

for i in $(seq 1 20); do
  STATUS=$(run_in_api curl -s "$API_BASE/tasks/$SLEEP_ID" | python3 -c 'import sys, json; print(json.load(sys.stdin)["status"])')
  if [ "$STATUS" = "cancelled" ]; then
    run_in_api curl -s "$API_BASE/tasks/$SLEEP_ID" | python3 -c 'import sys, json; print(json.load(sys.stdin))'
    break
  fi
  sleep 1
  if [ "$i" -eq 20 ]; then
    echo "sleep task did not cancel in time" >&2
    exit 1
  fi
done
