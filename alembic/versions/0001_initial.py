"""initial schema

Revision ID: 0001_initial
Revises: 
Create Date: 2026-02-06 04:00:00
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "0001_initial"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "tasks",
        sa.Column("id", sa.String(), primary_key=True),
        sa.Column("type", sa.String(), nullable=False),
        sa.Column("status", sa.String(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.Column("idempotency_key", sa.String(), nullable=True),
        sa.Column("params_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("result_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("timeout_seconds", sa.Integer(), nullable=False),
        sa.Column("max_retries", sa.Integer(), nullable=False),
        sa.Column("retry_count", sa.Integer(), nullable=False),
        sa.Column("started_at", sa.DateTime(), nullable=True),
        sa.Column("finished_at", sa.DateTime(), nullable=True),
        sa.Column("cancel_requested", sa.Boolean(), nullable=False),
        sa.UniqueConstraint("idempotency_key", name="uq_tasks_idempotency_key"),
    )
    op.create_index("ix_tasks_type", "tasks", ["type"])
    op.create_index("ix_tasks_status", "tasks", ["status"])
    op.create_index("ix_tasks_idempotency_key", "tasks", ["idempotency_key"])

    op.create_table(
        "artifacts",
        sa.Column("id", sa.String(), primary_key=True),
        sa.Column("task_id", sa.String(), sa.ForeignKey("tasks.id"), nullable=False),
        sa.Column("type", sa.String(), nullable=False),
        sa.Column("content_type", sa.String(), nullable=False),
        sa.Column("path", sa.String(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
    )
    op.create_index("ix_artifacts_task_id", "artifacts", ["task_id"])
    op.create_index("ix_artifacts_type", "artifacts", ["type"])

    op.create_table(
        "runs",
        sa.Column("id", sa.String(), primary_key=True),
        sa.Column("run_key", sa.String(), nullable=False),
        sa.Column("status", sa.String(), nullable=False),
        sa.Column("started_at", sa.DateTime(), nullable=False),
        sa.Column("finished_at", sa.DateTime(), nullable=True),
        sa.Column("metadata_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    )
    op.create_index("ix_runs_run_key", "runs", ["run_key"])
    op.create_index("ix_runs_status", "runs", ["status"])

    op.create_table(
        "metrics",
        sa.Column("id", sa.String(), primary_key=True),
        sa.Column("run_id", sa.String(), sa.ForeignKey("runs.id"), nullable=True),
        sa.Column("task_id", sa.String(), sa.ForeignKey("tasks.id"), nullable=True),
        sa.Column("name", sa.String(), nullable=False),
        sa.Column("value", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
    )
    op.create_index("ix_metrics_run_id", "metrics", ["run_id"])
    op.create_index("ix_metrics_task_id", "metrics", ["task_id"])
    op.create_index("ix_metrics_name", "metrics", ["name"])

    op.create_table(
        "notes",
        sa.Column("id", sa.String(), primary_key=True),
        sa.Column("run_id", sa.String(), sa.ForeignKey("runs.id"), nullable=True),
        sa.Column("task_id", sa.String(), sa.ForeignKey("tasks.id"), nullable=True),
        sa.Column("type", sa.String(), nullable=False),
        sa.Column("content", sa.Text(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
    )
    op.create_index("ix_notes_run_id", "notes", ["run_id"])
    op.create_index("ix_notes_task_id", "notes", ["task_id"])
    op.create_index("ix_notes_type", "notes", ["type"])


def downgrade() -> None:
    op.drop_index("ix_notes_type", table_name="notes")
    op.drop_index("ix_notes_task_id", table_name="notes")
    op.drop_index("ix_notes_run_id", table_name="notes")
    op.drop_table("notes")

    op.drop_index("ix_metrics_name", table_name="metrics")
    op.drop_index("ix_metrics_task_id", table_name="metrics")
    op.drop_index("ix_metrics_run_id", table_name="metrics")
    op.drop_table("metrics")

    op.drop_index("ix_runs_status", table_name="runs")
    op.drop_index("ix_runs_run_key", table_name="runs")
    op.drop_table("runs")

    op.drop_index("ix_artifacts_type", table_name="artifacts")
    op.drop_index("ix_artifacts_task_id", table_name="artifacts")
    op.drop_table("artifacts")

    op.drop_index("ix_tasks_idempotency_key", table_name="tasks")
    op.drop_index("ix_tasks_status", table_name="tasks")
    op.drop_index("ix_tasks_type", table_name="tasks")
    op.drop_table("tasks")
