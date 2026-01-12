import json
import logging
from datetime import datetime, timezone

from otel_worker.config import settings
from sqlalchemy import create_engine, text

logger = logging.getLogger(__name__)

# Using a single engine for simplicity in the worker
engine = create_engine(settings.POSTGRES_URL)


def init_db():
    """Initialize the otel schema and tables if they don't exist."""
    with engine.connect() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS otel;"))

        # Traces table
        conn.execute(
            text(
                """
            CREATE TABLE IF NOT EXISTS otel.traces (
                trace_id TEXT PRIMARY KEY,
                start_ts TIMESTAMPTZ,
                end_ts TIMESTAMPTZ,
                duration_ms BIGINT,
                service_name TEXT,
                environment TEXT,
                tenant_id TEXT NULL,
                interaction_id TEXT NULL,
                status TEXT,
                error_count INT,
                span_count INT,
                raw_blob_url TEXT,
                created_at TIMESTAMPTZ DEFAULT now()
            );
        """
            )
        )

        # Spans table
        conn.execute(
            text(
                """
            CREATE TABLE IF NOT EXISTS otel.spans (
                span_id TEXT PRIMARY KEY,
                trace_id TEXT REFERENCES otel.traces(trace_id),
                parent_span_id TEXT NULL,
                name TEXT,
                kind TEXT,
                start_ts TIMESTAMPTZ,
                end_ts TIMESTAMPTZ,
                duration_ms BIGINT,
                status TEXT,
                attributes JSONB,
                events JSONB
            );
        """
            )
        )
        conn.commit()
    logger.info("Initialized OTEL database schema and tables")


def save_trace_and_spans(trace_id: str, trace_data: dict, summaries: list[dict], raw_blob_url: str):
    """
    Save trace summary and its spans to Postgres.

    Idempotent upsert logic.
    """
    if not summaries:
        return

    # Calculate trace-level metrics
    service_name = summaries[0]["service_name"]
    start_ts = min(int(s["start_time_unix_nano"]) for s in summaries)
    end_ts = max(int(s["end_time_unix_nano"]) for s in summaries)
    duration_ms = (end_ts - start_ts) // 1_000_000

    # Convert nano timestamps to ISO string for PG
    start_dt = datetime.fromtimestamp(start_ts / 1e9, tz=timezone.utc)
    end_dt = datetime.fromtimestamp(end_ts / 1e9, tz=timezone.utc)

    # Simple error count by looking at status code
    error_count = sum(1 for s in summaries if s["status"] == "STATUS_CODE_ERROR")

    # Extract optional app-specific attributes from first span or resource (if present)
    tenant_id = None
    interaction_id = None
    for s in summaries:
        attrs = s.get("attributes", {})
        tenant_id = tenant_id or attrs.get("tenant_id")
        interaction_id = interaction_id or attrs.get("interaction_id")

    with engine.begin() as conn:
        # Upsert Trace
        conn.execute(
            text(
                """
            INSERT INTO otel.traces (
                trace_id, start_ts, end_ts, duration_ms, service_name,
                environment, tenant_id, interaction_id, status,
                error_count, span_count, raw_blob_url
            ) VALUES (
                :trace_id, :start_ts, :end_ts, :duration_ms, :service_name,
                :environment, :tenant_id, :interaction_id, :status,
                :error_count, :span_count, :raw_blob_url
            )
            ON CONFLICT (trace_id) DO UPDATE SET
                end_ts = EXCLUDED.end_ts,
                duration_ms = EXCLUDED.duration_ms,
                error_count = EXCLUDED.error_count,
                span_count = EXCLUDED.span_count,
                raw_blob_url = EXCLUDED.raw_blob_url;
        """
            ),
            {
                "trace_id": trace_id,
                "start_ts": start_dt,
                "end_ts": end_dt,
                "duration_ms": duration_ms,
                "service_name": service_name,
                "environment": settings.OTEL_ENVIRONMENT,
                "tenant_id": tenant_id,
                "interaction_id": interaction_id,
                "status": "ERROR" if error_count > 0 else "OK",
                "error_count": error_count,
                "span_count": len(summaries),
                "raw_blob_url": raw_blob_url,
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
                    start_ts, end_ts, duration_ms, status, attributes, events
                ) VALUES (
                    :span_id, :trace_id, :parent_span_id, :name, :kind,
                    :start_ts, :end_ts, :duration_ms, :status, :attributes, :events
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
                    "start_ts": s_start,
                    "end_ts": s_end,
                    "duration_ms": s_duration,
                    "status": s["status"],
                    "attributes": json.dumps(s.get("attributes", {})),
                    "events": json.dumps(s.get("events", [])),
                },
            )
    logger.info(f"Saved trace {trace_id} with {len(summaries)} spans to Postgres")
