import hashlib
import json
import logging
import os
from datetime import datetime, timezone

from sqlalchemy import create_engine, text

from otel_worker.config import settings
from otel_worker.storage.minio import upload_span_payload_blob

logger = logging.getLogger(__name__)

# Using a single engine for simplicity in the worker
engine = create_engine(settings.POSTGRES_URL)

# Get target schema from environment, default to 'otel'
TARGET_SCHEMA = os.getenv("OTEL_DB_SCHEMA", "otel")
MAX_INLINE_PAYLOAD_BYTES = 32 * 1024


def get_table_name(table: str) -> str:
    """Return table name with optional schema prefix."""
    if TARGET_SCHEMA:
        return f"{TARGET_SCHEMA}.{table}"
    return table


def init_db():
    """Validate that the schema and tables exist via migrations."""
    table_name = get_table_name("traces")
    try:
        with engine.connect() as conn:
            # Check for one of the core tables
            conn.execute(text(f"SELECT 1 FROM {table_name} LIMIT 1;"))
        logger.info(f"OTEL database table '{table_name}' validated")
    except Exception as e:
        logger.error(
            "OTEL schema validation failed. Ensure migrations are applied: `alembic upgrade head`"
        )
        raise RuntimeError(f"Missing OTEL schema: {e}")


def save_trace_and_spans(trace_id: str, trace_data: dict, summaries: list[dict], raw_blob_url: str):
    """
    Save trace summary and its spans to Postgres using the migration-hardened schema.

    Legacy wrapper for save_traces_batch for a single trace.
    """
    save_traces_batch(
        [{"trace_id": trace_id, "summaries": summaries, "raw_blob_url": raw_blob_url}]
    )


def save_traces_batch(trace_units: list[dict]):
    """Save multiple traces and their spans in a single Postgres transaction.

    Each unit should have: trace_id, summaries, raw_blob_url.
    """
    if not trace_units:
        return

    traces_table = get_table_name("traces")
    spans_table = get_table_name("spans")
    span_events_table = get_table_name("span_events")
    span_links_table = get_table_name("span_links")
    span_payloads_table = get_table_name("span_payloads")

    with engine.begin() as conn:
        for unit in trace_units:
            trace_id = unit["trace_id"]
            summaries = unit["summaries"]
            raw_blob_url = unit["raw_blob_url"]

            if not summaries:
                continue

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
                trace_attributes.update(attrs)

            # Upsert Trace
            conn.execute(
                text(
                    f"""
                INSERT INTO {traces_table} (
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
                    trace_attributes = ({traces_table}.trace_attributes::jsonb
                        || EXCLUDED.trace_attributes::jsonb)::json;
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

            # Upsert Spans and related entities
            for s in summaries:
                s_start = datetime.fromtimestamp(
                    int(s["start_time_unix_nano"]) / 1e9, tz=timezone.utc
                )
                s_end = datetime.fromtimestamp(int(s["end_time_unix_nano"]) / 1e9, tz=timezone.utc)
                s_duration = (
                    int(s["end_time_unix_nano"]) - int(s["start_time_unix_nano"])
                ) // 1_000_000

                conn.execute(
                    text(
                        f"""
                    INSERT INTO {spans_table} (
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

                # Skip span detail tables for lightweight SQLite test environment
                if engine.dialect.name == "sqlite":
                    continue

                # Span events
                for event in s.get("events", []) or []:
                    event_time = None
                    if event.get("time_unix_nano"):
                        event_time = datetime.fromtimestamp(
                            int(event["time_unix_nano"]) / 1e9, tz=timezone.utc
                        )
                    try:
                        conn.execute(
                            text(
                                f"""
                                INSERT INTO {span_events_table} (
                                    trace_id, span_id, event_name, event_time,
                                    attributes, dropped_attributes_count
                                ) VALUES (
                                    :trace_id, :span_id, :event_name, :event_time,
                                    :attributes, :dropped_attributes_count
                                )
                            """
                            ),
                            {
                                "trace_id": trace_id,
                                "span_id": s["span_id"],
                                "event_name": event.get("name", "event"),
                                "event_time": event_time,
                                "attributes": json.dumps(event.get("attributes", {})),
                                "dropped_attributes_count": event.get("dropped_attributes_count"),
                            },
                        )
                    except Exception as exc:
                        logger.warning(f"Failed to persist span event: {exc}")

                # Span links
                for link in s.get("links", []) or []:
                    try:
                        conn.execute(
                            text(
                                f"""
                                INSERT INTO {span_links_table} (
                                    trace_id, span_id, linked_trace_id, linked_span_id, attributes
                                ) VALUES (
                                    :trace_id, :span_id, :linked_trace_id,
                                    :linked_span_id, :attributes
                                )
                            """
                            ),
                            {
                                "trace_id": trace_id,
                                "span_id": s["span_id"],
                                "linked_trace_id": link.get("trace_id"),
                                "linked_span_id": link.get("span_id"),
                                "attributes": json.dumps(link.get("attributes", {})),
                            },
                        )
                    except Exception as exc:
                        logger.warning(f"Failed to persist span link: {exc}")

                # Span payloads (prompts, tool inputs/outputs, errors)
                payload_keys = [
                    "telemetry.inputs_json",
                    "telemetry.outputs_json",
                    "telemetry.error_json",
                    "llm.prompt.system",
                    "llm.prompt.user",
                    "llm.response.text",
                ]
                attrs = s.get("attributes", {}) or {}
                for key in payload_keys:
                    if key not in attrs:
                        continue

                    raw_payload = attrs.get(key)
                    payload_str = raw_payload
                    payload_obj = None
                    if isinstance(raw_payload, str):
                        payload_str = raw_payload
                        try:
                            payload_obj = json.loads(raw_payload)
                        except Exception:
                            payload_obj = raw_payload
                    else:
                        payload_obj = raw_payload
                        payload_str = json.dumps(raw_payload, default=str)

                    size_bytes = len(payload_str.encode("utf-8"))
                    payload_hash = hashlib.sha256(payload_str.encode("utf-8")).hexdigest()
                    blob_url = None
                    payload_json = payload_obj

                    if size_bytes > MAX_INLINE_PAYLOAD_BYTES:
                        try:
                            blob_url = upload_span_payload_blob(
                                trace_id, s["span_id"], key, payload_obj
                            )
                            payload_json = None
                        except Exception as exc:
                            logger.warning(f"Failed to upload payload blob: {exc}")

                    try:
                        conn.execute(
                            text(
                                f"""
                                INSERT INTO {span_payloads_table} (
                                    trace_id, span_id, payload_type, payload_json,
                                    blob_url, payload_hash, size_bytes, redacted
                                ) VALUES (
                                    :trace_id, :span_id, :payload_type, :payload_json,
                                    :blob_url, :payload_hash, :size_bytes, :redacted
                                )
                            """
                            ),
                            {
                                "trace_id": trace_id,
                                "span_id": s["span_id"],
                                "payload_type": key,
                                "payload_json": (
                                    json.dumps(payload_json) if payload_json is not None else None
                                ),
                                "blob_url": blob_url,
                                "payload_hash": payload_hash,
                                "size_bytes": size_bytes,
                                "redacted": False,
                            },
                        )
                    except Exception as exc:
                        logger.warning(f"Failed to persist span payload: {exc}")
    logger.info(f"Batched saved {len(trace_units)} traces to Postgres")


def list_traces(
    service: str = None,
    trace_id: str = None,
    start_time_gte: datetime = None,
    start_time_lte: datetime = None,
    limit: int = 50,
    offset: int = 0,
    order: str = "desc",
):
    """List traces from Postgres with filtering and pagination."""
    traces_table = get_table_name("traces")
    query = f"""
        SELECT trace_id, service_name, start_time, end_time,
               duration_ms, span_count, status, raw_blob_url
        FROM {traces_table}
        WHERE 1=1
    """
    params = {"limit": limit, "offset": offset}

    if service:
        query += " AND service_name = :service"
        params["service"] = service
    if trace_id:
        query += " AND trace_id = :trace_id"
        params["trace_id"] = trace_id
    if start_time_gte:
        query += " AND start_time >= :start_time_gte"
        params["start_time_gte"] = start_time_gte
    if start_time_lte:
        query += " AND start_time <= :start_time_lte"
        params["start_time_lte"] = start_time_lte

    query += f" ORDER BY start_time {order.upper()}"
    query += " LIMIT :limit OFFSET :offset"

    with engine.connect() as conn:
        result = conn.execute(text(query), params)
        return [dict(row._mapping) for row in result]


def _get_trace_metrics(conn, trace_id: str) -> dict:
    """Best-effort lookup for trace metrics (handles missing table in tests)."""
    metrics_table = get_table_name("trace_metrics")
    query = f"""
        SELECT total_tokens, prompt_tokens, completion_tokens, model_name, estimated_cost_usd
        FROM {metrics_table}
        WHERE trace_id = :trace_id
    """
    try:
        row = conn.execute(text(query), {"trace_id": trace_id}).fetchone()
    except Exception:
        return {}
    return dict(row._mapping) if row else {}


def get_trace(trace_id: str, include_attributes: bool = False):
    """Fetch a single trace by ID."""
    traces_table = get_table_name("traces")
    query = f"""
        SELECT trace_id, service_name, start_time, end_time, duration_ms, span_count,
               status, raw_blob_url, resource_attributes, trace_attributes
        FROM {traces_table}
        WHERE trace_id = :trace_id
    """
    with engine.connect() as conn:
        result = conn.execute(text(query), {"trace_id": trace_id})
        row = result.fetchone()
        if not row:
            return None

        data = dict(row._mapping)
        data.update(_get_trace_metrics(conn, trace_id))

        # Handle string JSON (e.g. from SQLite)
        for key in ["resource_attributes", "trace_attributes"]:
            val = data.get(key)
            if isinstance(val, str) and val.strip():
                try:
                    data[key] = json.loads(val)
                except Exception:
                    pass

        if not include_attributes:
            data.pop("resource_attributes", None)
            data.pop("trace_attributes", None)
        return data


def list_spans_for_trace(
    trace_id: str, limit: int = 200, offset: int = 0, include_attributes: bool = False
):
    """Fetch spans for a specific trace with pagination."""
    spans_table = get_table_name("spans")
    query = f"""
        SELECT span_id, trace_id, parent_span_id, name, kind, status_code,
               status_message, start_time, end_time, duration_ms, span_attributes, events
        FROM {spans_table}
        WHERE trace_id = :trace_id
        ORDER BY start_time ASC
        LIMIT :limit OFFSET :offset
    """
    params = {"trace_id": trace_id, "limit": limit, "offset": offset}

    with engine.connect() as conn:
        result = conn.execute(text(query), params)
        spans = []
        for row in result:
            data = dict(row._mapping)

            # Handle string JSON
            for key in ["span_attributes", "events"]:
                val = data.get(key)
                if isinstance(val, str) and val.strip():
                    try:
                        data[key] = json.loads(val)
                    except Exception:
                        pass

            if not include_attributes:
                data.pop("span_attributes", None)
                data.pop("events", None)
            spans.append(data)
        return spans


def resolve_trace_id_by_interaction(interaction_id: str) -> str | None:
    """Resolve a trace ID by interaction_id if available."""
    traces_table = get_table_name("traces")
    query = f"""
        SELECT trace_id
        FROM {traces_table}
        WHERE interaction_id = :interaction_id
        ORDER BY start_time DESC
        LIMIT 1
    """
    with engine.connect() as conn:
        row = conn.execute(text(query), {"interaction_id": interaction_id}).fetchone()
        return row[0] if row else None


def get_span_detail(trace_id: str, span_id: str) -> dict | None:
    """Fetch a span and its related events/links/payloads."""
    spans_table = get_table_name("spans")
    events_table = get_table_name("span_events")
    links_table = get_table_name("span_links")
    payloads_table = get_table_name("span_payloads")

    with engine.connect() as conn:
        row = conn.execute(
            text(
                f"""
                SELECT span_id, trace_id, parent_span_id, name, kind, status_code,
                       status_message, start_time, end_time, duration_ms, span_attributes, events
                FROM {spans_table}
                WHERE trace_id = :trace_id AND span_id = :span_id
            """
            ),
            {"trace_id": trace_id, "span_id": span_id},
        ).fetchone()

        if not row:
            return None

        data = dict(row._mapping)

        for key in ["span_attributes", "events"]:
            val = data.get(key)
            if isinstance(val, str) and val.strip():
                try:
                    data[key] = json.loads(val)
                except Exception:
                    pass

        data["links"] = []
        data["payloads"] = []

        if engine.dialect.name == "sqlite":
            return data

        try:
            event_rows = conn.execute(
                text(
                    f"""
                    SELECT event_name, event_time, attributes, dropped_attributes_count
                    FROM {events_table}
                    WHERE trace_id = :trace_id AND span_id = :span_id
                    ORDER BY event_time ASC
                """
                ),
                {"trace_id": trace_id, "span_id": span_id},
            ).fetchall()
            data["events"] = []
            for row in event_rows:
                ev = dict(row._mapping)
                if isinstance(ev.get("attributes"), str):
                    try:
                        ev["attributes"] = json.loads(ev["attributes"])
                    except Exception:
                        pass
                data["events"].append(ev)
        except Exception:
            pass

        try:
            link_rows = conn.execute(
                text(
                    f"""
                    SELECT linked_trace_id, linked_span_id, attributes
                    FROM {links_table}
                    WHERE trace_id = :trace_id AND span_id = :span_id
                """
                ),
                {"trace_id": trace_id, "span_id": span_id},
            ).fetchall()
            data["links"] = []
            for row in link_rows:
                link = dict(row._mapping)
                if isinstance(link.get("attributes"), str):
                    try:
                        link["attributes"] = json.loads(link["attributes"])
                    except Exception:
                        pass
                data["links"].append(link)
        except Exception:
            pass

        try:
            payload_rows = conn.execute(
                text(
                    f"""
                    SELECT payload_type, payload_json, blob_url, payload_hash, size_bytes, redacted
                    FROM {payloads_table}
                    WHERE trace_id = :trace_id AND span_id = :span_id
                """
                ),
                {"trace_id": trace_id, "span_id": span_id},
            ).fetchall()
            data["payloads"] = []
            for row in payload_rows:
                payload = dict(row._mapping)
                if isinstance(payload.get("payload_json"), str) and payload["payload_json"]:
                    try:
                        payload["payload_json"] = json.loads(payload["payload_json"])
                    except Exception:
                        pass
                data["payloads"].append(payload)
        except Exception:
            pass

        return data


def get_queue_depth() -> int:
    """Get the current number of pending items in the ingestion queue."""
    queue_table = get_table_name("ingestion_queue")
    with engine.connect() as conn:
        result = conn.execute(text(f"SELECT count(*) FROM {queue_table} WHERE status = 'pending'"))
        return result.scalar()


def enqueue_ingestion(payload_json: dict, trace_id: str = None) -> int:
    """Write raw OTLP payload to ingestion queue for durable buffering."""
    queue_table = get_table_name("ingestion_queue")
    with engine.begin() as conn:
        result = conn.execute(
            text(
                f"""
            INSERT INTO {queue_table} (payload_json, trace_id, status)
            VALUES (:payload_json, :trace_id, 'pending')
            RETURNING id
        """
            ),
            {"payload_json": json.dumps(payload_json), "trace_id": trace_id},
        )
        return result.scalar()


def poll_ingestion_queue(limit: int = 10) -> list[dict]:
    """Fetch pending items from the ingestion queue and mark them as processing."""
    queue_table = get_table_name("ingestion_queue")
    # Using a simple SELECT ... FOR UPDATE SKIP LOCKED if supported (Postgres 9.5+)
    # or just a simple update + return.
    # We'll use a transaction to claim items.
    with engine.begin() as conn:
        # Atomic claim: update status to 'processing' and return the rows
        query = f"""
            UPDATE {queue_table}
            SET status = 'processing', attempts = attempts + 1
            WHERE id IN (
                SELECT id FROM {queue_table}
                WHERE status = 'pending'
                   OR (status = 'failed' AND attempts < 5 AND next_attempt_at <= NOW())
                ORDER BY received_at ASC
                LIMIT :limit
                FOR UPDATE SKIP LOCKED
            )
            RETURNING id, payload_json, trace_id, attempts
        """
        result = conn.execute(text(query), {"limit": limit})
        items = []
        for row in result:
            data = dict(row._mapping)
            if isinstance(data["payload_json"], str):
                data["payload_json"] = json.loads(data["payload_json"])
            items.append(data)
        return items


def update_ingestion_status(item_id: int, status: str, error: str = None):
    """Update the status of an ingestion item after processing."""
    queue_table = get_table_name("ingestion_queue")
    with engine.begin() as conn:
        if status == "complete":
            # We can delete on success to keep the queue small,
            # or just mark it. Let's mark it for now but maybe delete in a cleanup job.
            conn.execute(
                text(f"UPDATE {queue_table} SET status = 'complete' WHERE id = :id"),
                {"id": item_id},
            )
        else:
            # Failed: set next attempt time
            conn.execute(
                text(
                    f"""
                UPDATE {queue_table}
                SET status = 'failed', error_message = :error,
                    next_attempt_at = NOW() + (interval '1 minute' * power(2, attempts))
                WHERE id = :id
            """
                ),
                {"id": item_id, "error": error},
            )
