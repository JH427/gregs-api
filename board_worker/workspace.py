from pathlib import Path

from board_worker.config import BoardWorkerConfig


def ensure_workspace(config: BoardWorkerConfig, task_id: str) -> str:
    root = Path(config.workspace_root).expanduser()
    path = root / task_id
    path.mkdir(parents=True, exist_ok=True)
    return str(path)
