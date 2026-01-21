"""Add golden_traces table.

Revision ID: 164385b74039
Revises: 726f71a24a95
Create Date: 2026-01-20 01:00:00.000000

"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "164385b74039"
down_revision = "726f71a24a95"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Create golden_traces table."""
    op.create_table(
        "golden_traces",
        sa.Column("trace_id", sa.String(), nullable=False),
        sa.Column(
            "promoted_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("promoted_by", sa.String(), nullable=True),
        sa.Column("reason", sa.Text(), nullable=True),
        sa.Column("labels", postgresql.JSONB, server_default="{}", nullable=False),
        sa.PrimaryKeyConstraint("trace_id"),
        sa.ForeignKeyConstraint(["trace_id"], ["otel.traces.trace_id"], ondelete="CASCADE"),
        schema="otel",
    )


def downgrade() -> None:
    """Drop golden_traces table."""
    op.drop_table("golden_traces", schema="otel")
