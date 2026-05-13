"""add board collaboration fields

Revision ID: 0005_board_collaboration_fields
Revises: 0004_add_board_tables
Create Date: 2026-05-12 00:00:00
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "0005_board_collaboration_fields"
down_revision = "0004_add_board_tables"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "board_tasks",
        sa.Column(
            "allowed_capabilities",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'[]'::jsonb"),
        ),
    )
    op.add_column(
        "board_tasks",
        sa.Column(
            "watchers",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'[]'::jsonb"),
        ),
    )
    op.add_column(
        "board_tasks",
        sa.Column(
            "contributors",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'[]'::jsonb"),
        ),
    )
    op.add_column(
        "board_comments",
        sa.Column(
            "metadata_json",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
    )


def downgrade() -> None:
    op.drop_column("board_comments", "metadata_json")
    op.drop_column("board_tasks", "contributors")
    op.drop_column("board_tasks", "watchers")
    op.drop_column("board_tasks", "allowed_capabilities")
