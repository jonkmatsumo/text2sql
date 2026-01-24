"""Baseline OTEL schema.

Revision ID: 163d8f446eb9
Revises:
Create Date: 2026-01-11 23:24:06.649311

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.engine.reflection import Inspector

# revision identifiers, used by Alembic.
revision: str = "163d8f446eb9"
down_revision: Union[str, Sequence[str], None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema to baseline."""
    # Get schema from context or default to otel
    target_schema = op.get_context().version_table_schema or "otel"

    # Ensure schema exists
    op.execute(sa.text(f"CREATE SCHEMA IF NOT EXISTS {target_schema}"))

    # Inspect current state to allow adoption of existing tables
    conn = op.get_bind()
    inspector = Inspector.from_engine(conn)
    tables = inspector.get_table_names(schema=target_schema)

    # --- Traces Table ---
    if "traces" not in tables:
        op.create_table(
            "traces",
            sa.Column("trace_id", sa.String(), primary_key=True),
            sa.Column("service_name", sa.String(), nullable=True),
            sa.Column("start_time", sa.DateTime(timezone=True), nullable=True),
            sa.Column("end_time", sa.DateTime(timezone=True), nullable=True),
            sa.Column("duration_ms", sa.BigInteger(), nullable=True),
            sa.Column("resource_attributes", sa.JSON(), nullable=True),
            sa.Column("trace_attributes", sa.JSON(), nullable=True),
            sa.Column("environment", sa.String(), nullable=True),
            sa.Column("tenant_id", sa.String(), nullable=True),
            sa.Column("interaction_id", sa.String(), nullable=True),
            sa.Column("status", sa.String(), nullable=True),
            sa.Column("error_count", sa.Integer(), nullable=True),
            sa.Column("span_count", sa.Integer(), nullable=True),
            sa.Column("raw_blob_url", sa.String(), nullable=True),
            sa.Column(
                "created_at",
                sa.DateTime(timezone=True),
                server_default=sa.func.now(),
            ),
            schema=target_schema,
        )
    else:
        # Adopt and align existing table
        columns = [c["name"] for c in inspector.get_columns("traces", schema=target_schema)]
        if "start_ts" in columns and "start_time" not in columns:
            op.rename_column("traces", "start_ts", "start_time", schema=target_schema)
        if "end_ts" in columns and "end_time" not in columns:
            op.rename_column("traces", "end_ts", "end_time", schema=target_schema)
        if "resource_attributes" not in columns:
            op.add_column(
                "traces",
                sa.Column("resource_attributes", sa.JSON(), nullable=True),
                schema=target_schema,
            )
        if "trace_attributes" not in columns:
            op.add_column(
                "traces",
                sa.Column("trace_attributes", sa.JSON(), nullable=True),
                schema=target_schema,
            )

    # --- Spans Table ---
    if "spans" not in tables:
        op.create_table(
            "spans",
            sa.Column("span_id", sa.String(), primary_key=True),
            sa.Column(
                "trace_id",
                sa.String(),
                sa.ForeignKey(f"{target_schema}.traces.trace_id"),
                nullable=False,
            ),
            sa.Column("parent_span_id", sa.String(), nullable=True),
            sa.Column("name", sa.String(), nullable=True),
            sa.Column("kind", sa.String(), nullable=True),
            sa.Column("status_code", sa.String(), nullable=True),
            sa.Column("status_message", sa.String(), nullable=True),
            sa.Column("start_time", sa.DateTime(timezone=True), nullable=True),
            sa.Column("end_time", sa.DateTime(timezone=True), nullable=True),
            sa.Column("duration_ms", sa.BigInteger(), nullable=True),
            sa.Column("span_attributes", sa.JSON(), nullable=True),
            sa.Column("events", sa.JSON(), nullable=True),
            sa.Column(
                "created_at",
                sa.DateTime(timezone=True),
                server_default=sa.func.now(),
            ),
            schema=target_schema,
        )
    else:
        # Adopt and align existing table
        columns = [c["name"] for c in inspector.get_columns("spans", schema=target_schema)]
        if "start_ts" in columns and "start_time" not in columns:
            op.rename_column("spans", "start_ts", "start_time", schema=target_schema)
        if "end_ts" in columns and "end_time" not in columns:
            op.rename_column("spans", "end_ts", "end_time", schema=target_schema)
        if "status" in columns and "status_code" not in columns:
            op.rename_column("spans", "status", "status_code", schema=target_schema)
        if "attributes" in columns and "span_attributes" not in columns:
            op.rename_column("spans", "attributes", "span_attributes", schema=target_schema)
        if "status_message" not in columns:
            op.add_column(
                "spans",
                sa.Column("status_message", sa.String(), nullable=True),
                schema=target_schema,
            )
        if "created_at" not in columns:
            op.add_column(
                "spans",
                sa.Column(
                    "created_at",
                    sa.DateTime(timezone=True),
                    server_default=sa.func.now(),
                ),
                schema=target_schema,
            )

    # --- Indexes ---
    op.create_index(
        "ix_otel_traces_start_time",
        "traces",
        [sa.text("start_time DESC")],
        schema=target_schema,
    )
    op.create_index(
        "ix_otel_traces_service_name_start_time",
        "traces",
        ["service_name", sa.text("start_time DESC")],
        schema=target_schema,
    )
    op.create_index("ix_otel_spans_trace_id", "spans", ["trace_id"], schema=target_schema)


def downgrade() -> None:
    """Downgrade schema."""
    # Get schema from context or default to otel
    target_schema = op.get_context().version_table_schema or "otel"
    op.drop_table("spans", schema=target_schema)
    op.drop_table("traces", schema=target_schema)
