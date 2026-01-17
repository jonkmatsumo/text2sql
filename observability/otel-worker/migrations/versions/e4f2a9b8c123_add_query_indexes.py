"""Add query optimization indexes.

Revision ID: e4f2a9b8c123
Revises: da39a3ee5e6b
Create Date: 2026-01-17 09:20:00.000000

This migration adds indexes identified during the trace UX investigation:
- otel.spans(parent_span_id): Tree traversal for span hierarchy
- otel.traces(interaction_id): Control-plane correlation lookups
- otel.traces(tenant_id, start_time DESC): Multi-tenant filtering
"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "e4f2a9b8c123"
down_revision = "da39a3ee5e6b"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Add query optimization indexes for trace and span lookups."""
    # Get target schema from context
    target_schema = op.get_context().version_table_schema or "otel"

    # Index for span tree traversal (parent-child relationships)
    op.create_index(
        "ix_otel_spans_parent_span_id",
        "spans",
        ["parent_span_id"],
        schema=target_schema,
    )

    # Index for control-plane correlation (interaction lookups)
    op.create_index(
        "ix_otel_traces_interaction_id",
        "traces",
        ["interaction_id"],
        schema=target_schema,
    )

    # Composite index for multi-tenant time-range queries
    op.create_index(
        "ix_otel_traces_tenant_id_start_time",
        "traces",
        ["tenant_id", sa.text("start_time DESC")],
        schema=target_schema,
    )


def downgrade() -> None:
    """Remove query optimization indexes."""
    target_schema = op.get_context().version_table_schema or "otel"

    op.drop_index("ix_otel_traces_tenant_id_start_time", table_name="traces", schema=target_schema)
    op.drop_index("ix_otel_traces_interaction_id", table_name="traces", schema=target_schema)
    op.drop_index("ix_otel_spans_parent_span_id", table_name="spans", schema=target_schema)
