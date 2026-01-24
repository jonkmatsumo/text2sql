"""Add metric regressions table.

Revision ID: 726f71a24a95
Revises: f503b8c9d012
Create Date: 2026-01-20 00:00:00.000000

"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "726f71a24a95"
down_revision = "f503b8c9d012"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Create metric_regressions table."""
    op.create_table(
        "metric_regressions",
        sa.Column("id", sa.String(), nullable=False),
        sa.Column(
            "computed_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("window_start", sa.DateTime(timezone=True), nullable=False),
        sa.Column("window_end", sa.DateTime(timezone=True), nullable=False),
        sa.Column("baseline_window_start", sa.DateTime(timezone=True), nullable=False),
        sa.Column("baseline_window_end", sa.DateTime(timezone=True), nullable=False),
        sa.Column("metric_name", sa.String(), nullable=False),
        sa.Column("dimensions", postgresql.JSONB, nullable=True),
        sa.Column("baseline_value", sa.Float(), nullable=False),
        sa.Column("candidate_value", sa.Float(), nullable=False),
        sa.Column("delta_abs", sa.Float(), nullable=False),
        sa.Column("delta_pct", sa.Float(), nullable=False),
        sa.Column("status", sa.String(), nullable=False),
        sa.Column("sample_size", sa.Integer(), nullable=False),
        sa.Column("top_trace_ids", postgresql.JSONB, nullable=True),
        sa.PrimaryKeyConstraint("id"),
        schema="otel",
    )

    op.create_index(
        "idx_metric_regressions_computed_at", "metric_regressions", ["computed_at"], schema="otel"
    )
    op.create_index(
        "idx_metric_regressions_status", "metric_regressions", ["status"], schema="otel"
    )


def downgrade() -> None:
    """Drop metric_regressions table."""
    op.drop_table("metric_regressions", schema="otel")
