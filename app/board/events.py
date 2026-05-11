import uuid
from datetime import datetime
from typing import Any, Optional

from sqlalchemy.orm import Session

from app.models import BoardEvent


def create_board_event(
    db: Session,
    event_type: str,
    actor: str,
    task_id: Optional[str] = None,
    payload: Optional[dict[str, Any]] = None,
) -> BoardEvent:
    event = BoardEvent(
        id=str(uuid.uuid4()),
        task_id=task_id,
        event_type=event_type,
        actor=actor,
        payload_json=payload or {},
        created_at=datetime.utcnow(),
    )
    db.add(event)
    db.commit()
    return event
