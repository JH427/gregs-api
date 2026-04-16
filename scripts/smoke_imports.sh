#!/usr/bin/env bash
set -euo pipefail

API_BASE="http://127.0.0.1:8000"

run_in_api() {
  docker compose exec -T api "$@"
}

wait_for_task() {
  local task_id="$1"
  local expected="$2"
  for _ in $(seq 1 30); do
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

PDF_PATH="$TMP_DIR/sample.pdf"
CHATGPT_PATH="$TMP_DIR/chatgpt-export.json"
CONTAINER_PDF_PATH="/tmp/sample-import.pdf"
CONTAINER_CHATGPT_PATH="/tmp/chatgpt-export.json"

python3 - <<'PY' "$PDF_PATH" "$CHATGPT_PATH"
import base64
import json
import sys

pdf_bytes = base64.b64decode(
    "JVBERi0xLjQKJcTl8uXrp/Og0MTGCjEgMCBvYmoKPDwgL1R5cGUgL0NhdGFsb2cgL1BhZ2VzIDIgMCBSID4+CmVuZG9iagoyIDAgb2JqCjw8IC9UeXBlIC9QYWdlcyAvS2lkcyBbMyAwIFJdIC9Db3VudCAxID4+CmVuZG9iagozIDAgb2JqCjw8IC9UeXBlIC9QYWdlIC9QYXJlbnQgMiAwIFIgL01lZGlhQm94IFswIDAgMzAwIDE0NF0gL0NvbnRlbnRzIDQgMCBSIC9SZXNvdXJjZXMgPDwgL0ZvbnQgPDwgL0YxIDUgMCBSID4+ID4+ID4+CmVuZG9iago0IDAgb2JqCjw8IC9MZW5ndGggNDQgPj4Kc3RyZWFtCkJUCi9GMSAyNCBUZgoxMDAgMTAwIFRkCihQaGFzZSA5QSBQREYpIFRqCkVUCmVuZHN0cmVhbQplbmRvYmoKNSAwIG9iago8PCAvVHlwZSAvRm9udCAvU3VidHlwZSAvVHlwZTEgL0Jhc2VGb250IC9IZWx2ZXRpY2EgPj4KZW5kb2JqCnhyZWYKMCA2CjAwMDAwMDAwMDAgNjU1MzUgZiAKMDAwMDAwMDAxNSAwMDAwMCBuIAowMDAwMDAwMDY0IDAwMDAwIG4gCjAwMDAwMDAxMjEgMDAwMDAgbiAKMDAwMDAwMDI0NyAwMDAwMCBuIAowMDAwMDAwMzQxIDAwMDAwIG4gCnRyYWlsZXIKPDwgL1NpemUgNiAvUm9vdCAxIDAgUiA+PgpzdGFydHhyZWYKNDEyCiUlRU9G"
)
with open(sys.argv[1], "wb") as f:
    f.write(pdf_bytes)

payload = [
    {
        "id": "conv-1",
        "title": "First conversation",
        "messages": [
            {"id": "m1", "role": "user", "create_time": 1, "content": {"parts": ["hello"]}},
            {"id": "m2", "role": "assistant", "create_time": 2, "content": {"parts": ["hi"]}},
        ],
    },
    {
        "id": "conv-2",
        "title": "Second conversation",
        "messages": [
            {"id": "m3", "role": "user", "create_time": 3, "content": {"parts": ["phase 9a"]}},
            {"id": "m4", "role": "assistant", "create_time": 4, "content": {"parts": ["imports"]}},
        ],
    },
]
with open(sys.argv[2], "w", encoding="utf-8") as f:
    json.dump(payload, f)
PY

docker compose cp "$PDF_PATH" "api:$CONTAINER_PDF_PATH" >/dev/null
docker compose cp "$CHATGPT_PATH" "api:$CONTAINER_CHATGPT_PATH" >/dev/null

FILE_TASK_ID=$(run_in_api curl -s -X POST "$API_BASE/import/files" \
  -F "file=@$CONTAINER_PDF_PATH;type=application/pdf" \
  | python3 -c 'import sys, json; print(json.load(sys.stdin)["task_id"])')

wait_for_task "$FILE_TASK_ID" "completed"

run_in_api curl -s "$API_BASE/tasks/$FILE_TASK_ID/artifacts" | python3 -c '
import sys, json
payload = json.load(sys.stdin)
assert any(item["type"] == "import_parsed_markdown" for item in payload["artifacts"]), payload
print(payload)
'

CHATGPT_TASK_ID=$(run_in_api curl -s -X POST "$API_BASE/import/chatgpt" \
  -H 'Content-Type: application/json' \
  --data-binary @"$CONTAINER_CHATGPT_PATH" \
  | python3 -c 'import sys, json; print(json.load(sys.stdin)["task_id"])')

wait_for_task "$CHATGPT_TASK_ID" "completed"

run_in_api curl -s "$API_BASE/tasks/$CHATGPT_TASK_ID/artifacts" | python3 -c '
import sys, json
payload = json.load(sys.stdin)
parsed = [item for item in payload["artifacts"] if item["type"] == "import_parsed_text"]
assert len(parsed) >= 2, payload
print(payload)
'
