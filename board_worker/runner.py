import argparse
import base64
import time

from board_worker.api_client import BoardApiClient
from board_worker.config import BoardWorkerConfig, load_config
from board_worker.hermes_executor import HermesExecutionResult, run_hermes
from board_worker.prompts import build_task_prompt
from board_worker.workspace import ensure_workspace


def _fetch_ready_tasks(client: BoardApiClient, config: BoardWorkerConfig) -> list[dict]:
    items: dict[str, dict] = {}
    agent_name = config.canonical_agent_name or config.agent_name
    assigned = client.get("/tasks", {"status": "ready", "assignee": agent_name, "limit": 50})
    for task in assigned.get("tasks", []):
        items[task["id"]] = task
    for capability in config.capabilities:
        payload = client.get("/tasks", {"status": "ready", "requested_capability": capability, "limit": 50})
        for task in payload.get("tasks", []):
            items[task["id"]] = task
    return sorted(items.values(), key=lambda item: (item["priority"], item["created_at"]))


def _post_result_comment(client: BoardApiClient, task_id: str, config: BoardWorkerConfig, execution: HermesExecutionResult) -> None:
    agent_name = config.canonical_agent_name or config.agent_name
    comment_body = execution.result or execution.stdout.strip() or execution.stderr.strip() or execution.status
    client.post(
        f"/tasks/{task_id}/comments",
        {"author": agent_name, "comment_type": "status", "body": comment_body[:8000]},
    )


def _post_logs_artifact(client: BoardApiClient, task_id: str, config: BoardWorkerConfig, execution: HermesExecutionResult) -> None:
    agent_name = config.canonical_agent_name or config.agent_name
    log_text = f"exit_code={execution.exit_code}\nruntime_seconds={execution.runtime_seconds:.3f}\n\nSTDOUT:\n{execution.stdout}\n\nSTDERR:\n{execution.stderr}\n"
    if len(log_text) < 512:
        return
    client.post(
        f"/tasks/{task_id}/artifacts",
        {
            "artifact_type": "board_worker_log",
            "content_type": "text/plain",
            "data_base64": base64.b64encode(log_text.encode("utf-8")).decode("ascii"),
            "metadata": {"runtime_seconds": round(execution.runtime_seconds, 3), "exit_code": execution.exit_code},
            "created_by": agent_name,
        },
    )


def _execute_task(client: BoardApiClient, config: BoardWorkerConfig, task: dict) -> None:
    task_id = task["id"]
    agent_name = config.canonical_agent_name or config.agent_name
    client.post(f"/tasks/{task_id}/claim", {"agent_name": agent_name, "claim_ttl_seconds": config.claim_ttl_seconds})
    client.post(f"/tasks/{task_id}/start", {"agent_name": agent_name})
    comments_payload = client.get(f"/tasks/{task_id}/comments")
    prompt = build_task_prompt(agent_name, config.host_name, task, comments_payload.get("comments", []))
    workspace = ensure_workspace(config, task_id)
    execution = run_hermes(config, prompt + f"\nWORKSPACE:\n{workspace}\n")
    client.post(f"/tasks/{task_id}/heartbeat", {"agent_name": agent_name, "claim_ttl_seconds": config.claim_ttl_seconds})
    _post_result_comment(client, task_id, config, execution)
    _post_logs_artifact(client, task_id, config, execution)

    metadata = {
        "runtime_seconds": round(execution.runtime_seconds, 3),
        "exit_code": execution.exit_code,
        "worker": "board_worker",
    }
    if execution.status == "done":
        client.post(f"/tasks/{task_id}/complete", {"agent_name": agent_name, "metadata": metadata})
    elif execution.status == "blocked":
        client.post(
            f"/tasks/{task_id}/block",
            {"agent_name": agent_name, "reason": execution.result[:500], "metadata": metadata},
        )
    else:
        client.post(
            f"/tasks/{task_id}/fail",
            {"agent_name": agent_name, "error": execution.result[:500], "metadata": metadata},
        )


def run_loop(config: BoardWorkerConfig) -> None:
    client = BoardApiClient(config)
    agent = client.register_agent()
    agent_id = agent["id"]
    last_heartbeat = 0.0
    while True:
        now = time.time()
        if now - last_heartbeat >= config.heartbeat_interval_seconds:
            client.heartbeat_agent(agent_id, "idle")
            last_heartbeat = now
        tasks = _fetch_ready_tasks(client, config)
        if tasks:
            _execute_task(client, config, tasks[0])
            client.heartbeat_agent(agent_id, "idle")
            last_heartbeat = time.time()
            continue
        time.sleep(config.poll_interval_seconds)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    args = parser.parse_args()
    config = load_config(args.config)
    run_loop(config)


if __name__ == "__main__":
    main()
