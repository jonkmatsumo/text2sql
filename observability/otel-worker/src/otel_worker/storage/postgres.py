import json
import logging
from datetime import datetime, timezone

from otel_worker.config import settings
from sqlalchemy import create_engine, text

logger = logging.getLogger(__name__)

# Using a single engine for simplicity in the worker
engine = create_engine(settings.POSTGRES_URL)


def init_db():
    """Validate that the otel schema and tables exist via migrations."""
    try:
        with engine.connect() as conn:
            # Check for one of the core tables
            conn.execute(text("SELECT 1 FROM otel.traces LIMIT 1;"))
        logger.info("OTEL database schema validated")
    except Exception as e:
        logger.error(
            "OTEL schema validation failed. Ensure migrations are applied: `alembic upgrade head`"
        )
        raise RuntimeError(f"Missing OTEL schema: {e}")


def save_trace_and_spans(trace_id: str, trace_data: dict, summaries: list[dict], raw_blob_url: str):
    """
    Save trace summary and its spans to Postgres using the migration-hardened schema.

    Idempotent upsert logic.
    """
    if not summaries:
        return

    # Calculate trace-level metrics
    service_name = summaries[0]["service_name"]
    resource_attributes = summaries[0].get("resource_attributes", {})
    start_ts = min(int(s["start_time_unix_nano"]) for s in summaries)
    end_ts = max(int(s["end_time_unix_nano"]) for s in summaries)
    duration_ms = (end_ts - start_ts) // 1_000_000

    # Convert nano timestamps to ISO string for PG
    start_dt = datetime.fromtimestamp(start_ts / 1e9, tz=timezone.utc)
    end_dt = datetime.fromtimestamp(end_ts / 1e9, tz=timezone.utc)

    # Simple error count by looking at status code
    error_count = sum(1 for s in summaries if s["status"] == "STATUS_CODE_ERROR")

    # Extract optional app-specific attributes and trace-level attributes
    tenant_id = None
    interaction_id = None
    trace_attributes = {}
    for s in summaries:
        attrs = s.get("attributes", {})
        tenant_id = tenant_id or attrs.get("tenant_id")
        interaction_id = interaction_id or attrs.get("interaction_id")
        # For trace_attributes, we'll take top-level attributes found in any span
        trace_attributes.update(attrs)

    with engine.begin() as conn:
        # Upsert Trace
        conn.execute(
            text(
                """
            INSERT INTO otel.traces (
                trace_id, start_time, end_time, duration_ms, service_name,
                environment, tenant_id, interaction_id, status,
                error_count, span_count, raw_blob_url,
                resource_attributes, trace_attributes
            ) VALUES (
                :trace_id, :start_time, :end_time, :duration_ms, :service_name,
                :environment, :tenant_id, :interaction_id, :status,
                :error_count, :span_count, :raw_blob_url,
                :resource_attributes, :trace_attributes
            )
            ON CONFLICT (trace_id) DO UPDATE SET
                end_time = EXCLUDED.end_time,
                duration_ms = EXCLUDED.duration_ms,
                error_count = EXCLUDED.error_count,
                span_count = EXCLUDED.span_count,
                raw_blob_url = EXCLUDED.raw_blob_url,
                trace_attributes = otel.traces.trace_attributes || EXCLUDED.trace_attributes;
        """
            ),
            {
                "trace_id": trace_id,
                "start_time": start_dt,
                "end_time": end_dt,
                "duration_ms": duration_ms,
                "service_name": service_name,
                "environment": settings.OTEL_ENVIRONMENT,
                "tenant_id": tenant_id,
                "interaction_id": interaction_id,
                "status": "ERROR" if error_count > 0 else "OK",
                "error_count": error_count,
                "span_count": len(summaries),
                "raw_blob_url": raw_blob_url,
                "resource_attributes": json.dumps(resource_attributes),
                "trace_attributes": json.dumps(trace_attributes),
            },
        )

        # Upsert Spans
        for s in summaries:
            s_start = datetime.fromtimestamp(int(s["start_time_unix_nano"]) / 1e9, tz=timezone.utc)
            s_end = datetime.fromtimestamp(int(s["end_time_unix_nano"]) / 1e9, tz=timezone.utc)
            s_duration = (
                int(s["end_time_unix_nano"]) - int(s["start_time_unix_nano"])
            ) // 1_000_000

            conn.execute(
                text(
                    """
                INSERT INTO otel.spans (
                    span_id, trace_id, parent_span_id, name, kind,
                    start_time, end_time, duration_ms, status_code,
                    status_message, span_attributes, events
                ) VALUES (
                    :span_id, :trace_id, :parent_span_id, :name, :kind,
                    :start_time, :end_time, :duration_ms, :status_code,
                    :status_message, :span_attributes, :events
                )
                ON CONFLICT (span_id) DO NOTHING;
            """
                ),
                {
                    "span_id": s["span_id"],
                    "trace_id": trace_id,
                    "parent_span_id": s.get("parent_span_id"),
                    "name": s["name"],
                    "kind": s.get("kind", "INTERNAL"),
                    "start_time": s_start,
                    "end_time": s_end,
                    "duration_ms": s_duration,
                    "status_code": s["status"],
                    "status_message": s.get("status_message"),
                    "span_attributes": json.dumps(s.get("attributes", {})),
                    "events": json.dumps(s.get("events", [])),
                },
            )
    logger.info(f"Saved trace {trace_id} with {len(summaries)} spans to Postgres")
