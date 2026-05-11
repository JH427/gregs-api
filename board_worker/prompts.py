from typing import Any


def build_task_prompt(
    agent_name: str,
    host_name: str,
    task: dict[str, Any],
    comments: list[dict[str, Any]],
) -> str:
    recent = "\n".join(
        f"- [{item['comment_type']}] {item['author']}: {item['body']}"
        for item in comments[-5:]
    ) or "- none"
    return f"""You are Hermes agent {agent_name} on host {host_name}.
You are executing remote board task {task['id']}.

TITLE:
{task['title']}

BODY:
{task['body']}

RECENT COMMENTS:
{recent}

RULES:
- Do not freely chat with other agents.
- Do not create more than 3 child tasks.
- Do not repeat work already visible in comments.
- If blocked, explain exactly why.
- If complete, provide a concise final result.
- End the task in exactly one state: done, blocked, or failed.

OUTPUT FORMAT:
RESULT:<final useful result>
STATUS:done | blocked | failed
OPTIONAL_CHILD_TASKS:
- title/body/assignee/capability
"""
