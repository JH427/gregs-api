from datetime import datetime
from sqlalchemy import Boolean, Column, DateTime, Integer, String, Text, ForeignKey
from sqlalchemy.dialects.postgresql import JSONB
from app.db import Base


class Task(Base):
    __tablename__ = "tasks"

    id = Column(String, primary_key=True, index=True)
    type = Column(String, index=True, nullable=False)
    status = Column(String, index=True, nullable=False)

    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow)

    idempotency_key = Column(String, unique=True, nullable=True, index=True)

    params_json = Column(JSONB, nullable=False, default=dict)
    result_json = Column(JSONB, nullable=True)
    error = Column(Text, nullable=True)

    timeout_seconds = Column(Integer, nullable=False, default=300)
    max_retries = Column(Integer, nullable=False, default=0)
    retry_count = Column(Integer, nullable=False, default=0)

    started_at = Column(DateTime, nullable=True)
    finished_at = Column(DateTime, nullable=True)

    cancel_requested = Column(Boolean, nullable=False, default=False)


class Artifact(Base):
    __tablename__ = "artifacts"

    id = Column(String, primary_key=True, index=True)
    task_id = Column(String, ForeignKey("tasks.id"), nullable=False, index=True)
    type = Column(String, nullable=False, index=True)
    content_type = Column(String, nullable=False)
    path = Column(String, nullable=False)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)


class Run(Base):
    __tablename__ = "runs"

    id = Column(String, primary_key=True, index=True)
    run_key = Column(String, index=True, nullable=False)
    status = Column(String, index=True, nullable=False)
    started_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    finished_at = Column(DateTime, nullable=True)
    metadata_json = Column(JSONB, nullable=True)


class Metric(Base):
    __tablename__ = "metrics"

    id = Column(String, primary_key=True, index=True)
    run_id = Column(String, ForeignKey("runs.id"), nullable=True, index=True)
    task_id = Column(String, ForeignKey("tasks.id"), nullable=True, index=True)
    name = Column(String, nullable=False, index=True)
    value = Column(JSONB, nullable=False)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)


class Note(Base):
    __tablename__ = "notes"

    id = Column(String, primary_key=True, index=True)
    run_id = Column(String, ForeignKey("runs.id"), nullable=True, index=True)
    task_id = Column(String, ForeignKey("tasks.id"), nullable=True, index=True)
    type = Column(String, nullable=False, index=True)
    content = Column(Text, nullable=False)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
