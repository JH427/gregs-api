"""add board tables

Revision ID: 0004_add_board_tables
Revises: 0003_knowledge_promotion
Create Date: 2026-05-11 00:00:00
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "0004_add_board_tables"
down_revision = "0003_knowledge_promotion"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "board_agents",
        sa.Column("id", sa.String(), primary_key=True),
        sa.Column("name", sa.String(), nullable=False),
        sa.Column("host", sa.String(), nullable=False),
        sa.Column("capabilities", postgresql.JSONB(astext_type=sa.Text()), nullable=False, server_default=sa.text("'[]'::jsonb")),
        sa.Column("status", sa.String(), nullable=False, server_default=sa.text("'offline'")),
        sa.Column("last_heartbeat", sa.DateTime(timezone=True), nullable=True),
        sa.Column("enabled", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("metadata_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
    )
    op.create_index("ix_board_agents_name", "board_agents", ["name"])
    op.create_index("ix_board_agents_host", "board_agents", ["host"])
    op.create_index("ix_board_agents_status", "board_agents", ["status"])

    op.create_table(
        "board_tasks",
        sa.Column("id", sa.String(), primary_key=True),
        sa.Column("title", sa.Text(), nullable=False),
        sa.Column("body", sa.Text(), nullable=False),
        sa.Column("status", sa.String(), nullable=False, server_default=sa.text("'triage'")),
        sa.Column("priority", sa.Integer(), nullable=False, server_default=sa.text("3")),
        sa.Column("assignee", sa.String(), nullable=True),
        sa.Column("requested_capability", sa.String(), nullable=True),
        sa.Column("created_by", sa.String(), nullable=False, server_default=sa.text("'user'")),
        sa.Column("claimed_by", sa.String(), nullable=True),
        sa.Column("claim_expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("parent_task_id", sa.String(), sa.ForeignKey("board_tasks.id"), nullable=True),
        sa.Column("workspace_type", sa.String(), nullable=True, server_default=sa.text("'scratch'")),
        sa.Column("workspace_ref", sa.String(), nullable=True),
        sa.Column("max_retries", sa.Integer(), nullable=False, server_default=sa.text("2")),
        sa.Column("retry_count", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("idempotency_key", sa.String(), nullable=True),
        sa.Column("metadata_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.UniqueConstraint("idempotency_key", name="uq_board_tasks_idempotency_key"),
    )
    op.create_index("ix_board_tasks_status", "board_tasks", ["status"])
    op.create_index("ix_board_tasks_priority", "board_tasks", ["priority"])
    op.create_index("ix_board_tasks_assignee", "board_tasks", ["assignee"])
    op.create_index("ix_board_tasks_requested_capability", "board_tasks", ["requested_capability"])
    op.create_index("ix_board_tasks_claimed_by", "board_tasks", ["claimed_by"])
    op.create_index("ix_board_tasks_claim_expires_at", "board_tasks", ["claim_expires_at"])
    op.create_index("ix_board_tasks_parent_task_id", "board_tasks", ["parent_task_id"])
    op.create_index("ix_board_tasks_idempotency_key", "board_tasks", ["idempotency_key"])

    op.create_table(
        "board_comments",
        sa.Column("id", sa.String(), primary_key=True),
        sa.Column("task_id", sa.String(), sa.ForeignKey("board_tasks.id"), nullable=False),
        sa.Column("author", sa.String(), nullable=False),
        sa.Column("comment_type", sa.String(), nullable=False, server_default=sa.text("'info'")),
        sa.Column("body", sa.Text(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
    )
    op.create_index("ix_board_comments_task_id", "board_comments", ["task_id"])
    op.create_index("ix_board_comments_author", "board_comments", ["author"])
    op.create_index("ix_board_comments_comment_type", "board_comments", ["comment_type"])

    op.create_table(
        "board_events",
        sa.Column("id", sa.String(), primary_key=True),
        sa.Column("task_id", sa.String(), sa.ForeignKey("board_tasks.id"), nullable=True),
        sa.Column("event_type", sa.String(), nullable=False),
        sa.Column("actor", sa.String(), nullable=False),
        sa.Column("payload_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
    )
    op.create_index("ix_board_events_task_id", "board_events", ["task_id"])
    op.create_index("ix_board_events_event_type", "board_events", ["event_type"])
    op.create_index("ix_board_events_actor", "board_events", ["actor"])

    op.create_table(
        "board_task_artifacts",
        sa.Column("id", sa.String(), primary_key=True),
        sa.Column("task_id", sa.String(), sa.ForeignKey("board_tasks.id"), nullable=False),
        sa.Column("artifact_id", sa.String(), sa.ForeignKey("artifacts.id"), nullable=True),
        sa.Column("artifact_type", sa.String(), nullable=True),
        sa.Column("path", sa.String(), nullable=True),
        sa.Column("metadata_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("created_by", sa.String(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
    )
    op.create_index("ix_board_task_artifacts_task_id", "board_task_artifacts", ["task_id"])
    op.create_index("ix_board_task_artifacts_artifact_id", "board_task_artifacts", ["artifact_id"])
    op.create_index("ix_board_task_artifacts_artifact_type", "board_task_artifacts", ["artifact_type"])
    op.create_index("ix_board_task_artifacts_created_by", "board_task_artifacts", ["created_by"])


def downgrade() -> None:
    op.drop_index("ix_board_task_artifacts_created_by", table_name="board_task_artifacts")
    op.drop_index("ix_board_task_artifacts_artifact_type", table_name="board_task_artifacts")
    op.drop_index("ix_board_task_artifacts_artifact_id", table_name="board_task_artifacts")
    op.drop_index("ix_board_task_artifacts_task_id", table_name="board_task_artifacts")
    op.drop_table("board_task_artifacts")

    op.drop_index("ix_board_events_actor", table_name="board_events")
    op.drop_index("ix_board_events_event_type", table_name="board_events")
    op.drop_index("ix_board_events_task_id", table_name="board_events")
    op.drop_table("board_events")

    op.drop_index("ix_board_comments_comment_type", table_name="board_comments")
    op.drop_index("ix_board_comments_author", table_name="board_comments")
    op.drop_index("ix_board_comments_task_id", table_name="board_comments")
    op.drop_table("board_comments")

    op.drop_index("ix_board_tasks_idempotency_key", table_name="board_tasks")
    op.drop_index("ix_board_tasks_parent_task_id", table_name="board_tasks")
    op.drop_index("ix_board_tasks_claim_expires_at", table_name="board_tasks")
    op.drop_index("ix_board_tasks_claimed_by", table_name="board_tasks")
    op.drop_index("ix_board_tasks_requested_capability", table_name="board_tasks")
    op.drop_index("ix_board_tasks_assignee", table_name="board_tasks")
    op.drop_index("ix_board_tasks_priority", table_name="board_tasks")
    op.drop_index("ix_board_tasks_status", table_name="board_tasks")
    op.drop_table("board_tasks")

    op.drop_index("ix_board_agents_status", table_name="board_agents")
    op.drop_index("ix_board_agents_host", table_name="board_agents")
    op.drop_index("ix_board_agents_name", table_name="board_agents")
    op.drop_table("board_agents")
