"""Add ingestion queue.

Revision ID: cf560123d456
Revises: 163d8f446eb9
Create Date: 2026-01-13 14:00:00.000000

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "cf560123d456"
down_revision: Union[str, Sequence[str], None] = "163d8f446eb9"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Add ingestion_queue table."""
    target_schema = op.get_context().version_table_schema or "otel"

    op.create_table(
        "ingestion_queue",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("received_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column(
            "status", sa.String(), server_default="pending"
        ),  # pending, processing, complete, failed
        sa.Column("attempts", sa.Integer(), server_default="0"),
        sa.Column("next_attempt_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("payload_json", sa.JSON(), nullable=False),
        sa.Column("trace_id", sa.String(), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
        schema=target_schema,
    )

    op.create_index(
        "ix_otel_ingestion_queue_status_next_attempt",
        "ingestion_queue",
        ["status", "next_attempt_at"],
        schema=target_schema,
    )


def downgrade() -> None:
    """Remove ingestion_queue table."""
    target_schema = op.get_context().version_table_schema or "otel"
    op.drop_table("ingestion_queue", schema=target_schema)
