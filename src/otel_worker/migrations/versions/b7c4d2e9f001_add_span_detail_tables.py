"""Add span detail tables and trace metrics columns.

Revision ID: b7c4d2e9f001
Revises: 164385b74039
Create Date: 2026-01-27 00:00:00.000000

"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "b7c4d2e9f001"
down_revision = "164385b74039"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Create span_events, span_links, span_payloads, and extend trace_metrics."""
    target_schema = op.get_context().version_table_schema or "otel"

    # Span events
    op.create_table(
        "span_events",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("trace_id", sa.String(), nullable=False),
        sa.Column("span_id", sa.String(), nullable=False),
        sa.Column("event_name", sa.String(), nullable=False),
        sa.Column("event_time", sa.DateTime(timezone=True), nullable=True),
        sa.Column("attributes", postgresql.JSONB, nullable=True),
        sa.Column("dropped_attributes_count", sa.Integer(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["trace_id"], [f"{target_schema}.traces.trace_id"], ondelete="CASCADE"
        ),
        schema=target_schema,
    )

    op.create_index(
        "ix_otel_span_events_trace_span",
        "span_events",
        ["trace_id", "span_id"],
        schema=target_schema,
    )

    # Span links
    op.create_table(
        "span_links",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("trace_id", sa.String(), nullable=False),
        sa.Column("span_id", sa.String(), nullable=False),
        sa.Column("linked_trace_id", sa.String(), nullable=False),
        sa.Column("linked_span_id", sa.String(), nullable=True),
        sa.Column("attributes", postgresql.JSONB, nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["trace_id"], [f"{target_schema}.traces.trace_id"], ondelete="CASCADE"
        ),
        schema=target_schema,
    )

    op.create_index(
        "ix_otel_span_links_trace_span",
        "span_links",
        ["trace_id", "span_id"],
        schema=target_schema,
    )
    op.create_index(
        "ix_otel_span_links_linked_trace",
        "span_links",
        ["linked_trace_id"],
        schema=target_schema,
    )

    # Span payloads
    op.create_table(
        "span_payloads",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("trace_id", sa.String(), nullable=False),
        sa.Column("span_id", sa.String(), nullable=False),
        sa.Column("payload_type", sa.String(), nullable=False),
        sa.Column("payload_json", postgresql.JSONB, nullable=True),
        sa.Column("blob_url", sa.String(), nullable=True),
        sa.Column("payload_hash", sa.String(), nullable=True),
        sa.Column("size_bytes", sa.Integer(), nullable=True),
        sa.Column("redacted", sa.Boolean(), server_default="false", nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["trace_id"], [f"{target_schema}.traces.trace_id"], ondelete="CASCADE"
        ),
        schema=target_schema,
    )

    op.create_index(
        "ix_otel_span_payloads_trace_span",
        "span_payloads",
        ["trace_id", "span_id"],
        schema=target_schema,
    )

    # Trace metrics extensions
    op.add_column(
        "trace_metrics",
        sa.Column("model_name", sa.String(), nullable=True),
        schema=target_schema,
    )
    op.add_column(
        "trace_metrics",
        sa.Column("estimated_cost_usd", sa.Float(), nullable=True),
        schema=target_schema,
    )


def downgrade() -> None:
    """Drop span detail tables and trace metrics columns."""
    target_schema = op.get_context().version_table_schema or "otel"

    op.drop_column("trace_metrics", "estimated_cost_usd", schema=target_schema)
    op.drop_column("trace_metrics", "model_name", schema=target_schema)

    op.drop_table("span_payloads", schema=target_schema)
    op.drop_table("span_links", schema=target_schema)
    op.drop_table("span_events", schema=target_schema)
