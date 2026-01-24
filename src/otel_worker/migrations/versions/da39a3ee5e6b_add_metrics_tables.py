"""Add metrics tables.

Revision ID: da39a3ee5e6b
Revises: cf560123d456
Create Date: 2026-01-16 00:00:00.000000

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "da39a3ee5e6b"
down_revision = "cf560123d456"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Create trace_metrics and stage_metrics tables."""
    # 1. Trace Metrics Table
    op.create_table(
        "trace_metrics",
        sa.Column("trace_id", sa.String(), nullable=False),
        sa.Column("service_name", sa.String(), nullable=True),
        sa.Column("start_time", sa.DateTime(timezone=True), nullable=True),
        sa.Column("end_time", sa.DateTime(timezone=True), nullable=True),
        sa.Column("duration_ms", sa.BigInteger(), nullable=True),
        sa.Column("has_error", sa.Boolean(), server_default="false", nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("trace_id"),
        schema="otel",
    )

    # 2. Stage Metrics Table
    op.create_table(
        "stage_metrics",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("trace_id", sa.String(), nullable=False),
        sa.Column("stage", sa.String(), nullable=False),
        sa.Column("duration_ms", sa.BigInteger(), nullable=True),
        sa.Column("has_error", sa.Boolean(), server_default="false", nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.ForeignKeyConstraint(["trace_id"], ["otel.trace_metrics.trace_id"], ondelete="CASCADE"),
        schema="otel",
    )

    # Indexes for common lookups
    op.create_index("idx_trace_metrics_start_time", "trace_metrics", ["start_time"], schema="otel")
    op.create_index("idx_stage_metrics_stage", "stage_metrics", ["stage"], schema="otel")


def downgrade() -> None:
    """Drop metrics tables."""
    op.drop_table("stage_metrics", schema="otel")
    op.drop_table("trace_metrics", schema="otel")
