"""Add token usage metrics.

Revision ID: f503b8c9d012
Revises: e4f2a9b8c123
Create Date: 2026-01-19 18:00:00.000000

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "f503b8c9d012"
down_revision = "e4f2a9b8c123"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Add token usage columns to trace_metrics."""
    op.add_column(
        "trace_metrics",
        sa.Column("total_tokens", sa.BigInteger(), nullable=True),
        schema="otel",
    )
    op.add_column(
        "trace_metrics",
        sa.Column("prompt_tokens", sa.BigInteger(), nullable=True),
        schema="otel",
    )
    op.add_column(
        "trace_metrics",
        sa.Column("completion_tokens", sa.BigInteger(), nullable=True),
        schema="otel",
    )


def downgrade() -> None:
    """Remove token usage columns."""
    op.drop_column("trace_metrics", "completion_tokens", schema="otel")
    op.drop_column("trace_metrics", "prompt_tokens", schema="otel")
    op.drop_column("trace_metrics", "total_tokens", schema="otel")
