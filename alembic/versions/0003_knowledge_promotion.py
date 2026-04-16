"""add knowledge promotion schema

Revision ID: 0003_knowledge_promotion
Revises: 0002_artifact_metadata
Create Date: 2026-04-09 00:00:00
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "0003_knowledge_promotion"
down_revision = "0002_artifact_metadata"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "knowledge_documents",
        sa.Column("id", sa.String(), primary_key=True),
        sa.Column("artifact_id", sa.String(), nullable=False),
        sa.Column("domain", sa.String(), nullable=False),
        sa.Column("source", sa.String(), nullable=True),
        sa.Column("confidence", sa.String(), nullable=True),
        sa.Column("promotion_key", sa.String(), nullable=False),
        sa.Column("embedding_model", sa.String(), nullable=False),
        sa.Column("embedding_revision", sa.String(), nullable=False),
        sa.Column("chunker_version", sa.String(), nullable=False),
        sa.Column("chunk_params", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("promoted_at", sa.DateTime(timezone=True), nullable=True),
        sa.UniqueConstraint("promotion_key", name="uq_knowledge_documents_promotion_key"),
    )
    op.create_index("ix_knowledge_documents_artifact_id", "knowledge_documents", ["artifact_id"])
    op.create_index("ix_knowledge_documents_domain", "knowledge_documents", ["domain"])

    op.create_table(
        "knowledge_chunks",
        sa.Column("id", sa.String(), primary_key=True),
        sa.Column("document_id", sa.String(), sa.ForeignKey("knowledge_documents.id"), nullable=False),
        sa.Column("chunk_index", sa.Integer(), nullable=False),
        sa.Column("qdrant_point_id", sa.String(), nullable=False),
        sa.Column("text_artifact_id", sa.String(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
    )
    op.create_index("ix_knowledge_chunks_document_id", "knowledge_chunks", ["document_id"])


def downgrade() -> None:
    op.drop_index("ix_knowledge_chunks_document_id", table_name="knowledge_chunks")
    op.drop_table("knowledge_chunks")

    op.drop_index("ix_knowledge_documents_domain", table_name="knowledge_documents")
    op.drop_index("ix_knowledge_documents_artifact_id", table_name="knowledge_documents")
    op.drop_table("knowledge_documents")
