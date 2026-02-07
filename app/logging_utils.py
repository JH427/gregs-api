import json
import logging
import logging.config
import os
import sys
from datetime import datetime
from typing import Any, Dict

STANDARD_ATTRS = {
    "name",
    "msg",
    "args",
    "levelname",
    "levelno",
    "pathname",
    "filename",
    "module",
    "exc_info",
    "exc_text",
    "stack_info",
    "lineno",
    "funcName",
    "created",
    "msecs",
    "relativeCreated",
    "thread",
    "threadName",
    "processName",
    "process",
}


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: Dict[str, Any] = {
            "ts": datetime.utcnow().isoformat() + "Z",
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        for key, value in record.__dict__.items():
            if key in STANDARD_ATTRS:
                continue
            payload[key] = value
        return json.dumps(payload)


def configure_logging() -> None:
    config_path = os.getenv("LOG_CONFIG", "/app/app/logging_config.json")
    try:
        with open(config_path, "r", encoding="utf-8") as f:
            config = json.load(f)
        logging.config.dictConfig(config)
    except Exception:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(JsonFormatter())
        root = logging.getLogger()
        root.handlers = [handler]
        root.setLevel(logging.INFO)


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)


def log_event(logger: logging.Logger, event: str, **fields: Any) -> None:
    safe_fields: Dict[str, Any] = {}
    for key, value in fields.items():
        if key in STANDARD_ATTRS:
            safe_fields[f"field_{key}"] = value
        else:
            safe_fields[key] = value
    logger.info(event, extra={"event": event, **safe_fields})


def log_task_transition(
    logger: logging.Logger,
    task_id: str,
    task_type: str,
    from_status: str,
    to_status: str,
    **fields: Any,
) -> None:
    logger.info(
        "task_state",
        extra={
            "event": "task_state",
            "task_id": task_id,
            "type": task_type,
            "from_status": from_status,
            "to_status": to_status,
            **fields,
        },
    )
