import base64
import hashlib
import json
import logging
import math
import os
import threading
from collections import deque
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


class SafeIngestQueue:
    """Manages ingestion with an in-memory buffer fallback during Postgres spikes."""

    def __init__(self, max_buffer_size: int = 5000):
        """Initialize the safe ingest queue."""
        self.buffer = deque(maxlen=max_buffer_size)
        self._lock = threading.Lock()
        self._worker_thread = None
        self._stopping = False

    def start(self):
        """Start the background drain worker."""
        self._stopping = False
        self._worker_thread = threading.Thread(target=self._drain_loop, daemon=True)
        self._worker_thread.start()
        logger.info("SafeIngestQueue drain worker started")

    def stop(self):
        """Stop the background drain worker."""
        self._stopping = True
        if self._worker_thread:
            self._worker_thread.join(timeout=5.0)

    def enqueue(self, payload_json: dict, trace_id: str = None):
        """Enqueue the payload to Postgres, falling back to memory if it fails."""
        try:
            return enqueue_ingestion_direct(payload_json, trace_id)
        except Exception as e:
            logger.warning(f"Postgres ingestion failed, buffering in memory: {e}")
            with self._lock:
                self.buffer.append({"payload": payload_json, "trace_id": trace_id})
            return -1

    def _drain_loop(self):
        """Periodically flush memory buffer to Postgres."""
        while not self._stopping:
            item = None
            with self._lock:
                if self.buffer:
                    item = self.buffer[0]  # Peek

            if item:
                try:
                    enqueue_ingestion_direct(item["payload"], item["trace_id"])
                    with self._lock:
                        self.buffer.popleft()  # Success, remove it
                    logger.info(
                        f"Recovered buffered ingestion item (Remaining: {len(self.buffer)})"
                    )
                except Exception:
                    # PG still down, wait before retry
                    threading.Event().wait(10.0)
            else:
                threading.Event().wait(2.0)


# Global instance
safe_queue = SafeIngestQueue()


def _decode_trace_id(trace_id: str | None) -> str | None:
    """Decode base64 OTLP trace IDs to hex when possible."""
    if not trace_id:
        return None
    if all(c in "0123456789abcdef" for c in trace_id.lower()) and len(trace_id) == 32:
        return trace_id.lower()
    try:
        return base64.b64decode(trace_id).hex()
    except Exception:
        return trace_id


def get_table_name(table: str) -> str:
    """Return the table name with an optional schema prefix."""
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


def save_traces_batch(trace_units: list[dict]):
    """Save multiple traces and their spans in a single Postgres transaction."""
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
                                "linked_trace_id": _decode_trace_id(link.get("trace_id")),
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


def _build_trace_filter_clause(
    service: str = None,
    trace_id: str = None,
    status: str = None,
    has_errors: str = None,
    start_time_gte: datetime = None,
    start_time_lte: datetime = None,
    duration_min_ms: int = None,
    duration_max_ms: int = None,
):
    clauses = []
    params = {}

    if service:
        clauses.append("service_name = :service")
        params["service"] = service
    if trace_id:
        clauses.append("trace_id = :trace_id")
        params["trace_id"] = trace_id
    if status:
        clauses.append("status = :status")
        params["status"] = status
    if has_errors == "yes":
        clauses.append("error_count > 0")
    elif has_errors == "no":
        clauses.append("error_count = 0")
    if start_time_gte:
        clauses.append("start_time >= :start_time_gte")
        params["start_time_gte"] = start_time_gte
    if start_time_lte:
        clauses.append("start_time <= :start_time_lte")
        params["start_time_lte"] = start_time_lte
    if duration_min_ms is not None:
        clauses.append("duration_ms >= :duration_min_ms")
        params["duration_min_ms"] = duration_min_ms
    if duration_max_ms is not None:
        clauses.append("duration_ms <= :duration_max_ms")
        params["duration_max_ms"] = duration_max_ms

    where = " AND ".join(clauses) if clauses else "1=1"
    return where, params


def _compute_histogram_bins(values: list[int], bin_count: int = 20):
    if not values:
        return []
    min_val = min(values)
    max_val = max(values)
    span = max(1, max_val - min_val)
    width = max(1, math.ceil(span / bin_count))
    bins = [
        {"start_ms": min_val + i * width, "end_ms": min_val + (i + 1) * width, "count": 0}
        for i in range(bin_count)
    ]
    for value in values:
        idx = min(bin_count - 1, max(0, (value - min_val) // width))
        bins[int(idx)]["count"] += 1
    return bins


def _compute_percentiles(values: list[int]):
    if not values:
        return {"p50_ms": None, "p95_ms": None, "p99_ms": None}
    sorted_vals = sorted(values)

    def pick(pct: float) -> int:
        idx = max(0, min(len(sorted_vals) - 1, math.ceil((pct / 100) * len(sorted_vals)) - 1))
        return int(sorted_vals[idx])

    return {"p50_ms": pick(50), "p95_ms": pick(95), "p99_ms": pick(99)}


def compute_trace_aggregations(
    service: str = None,
    trace_id: str = None,
    status: str = None,
    has_errors: str = None,
    start_time_gte: datetime = None,
    start_time_lte: datetime = None,
    duration_min_ms: int = None,
    duration_max_ms: int = None,
    bin_count: int = 20,
):
    """Compute trace aggregation data for search facets and histograms."""
    traces_table = get_table_name("traces")
    where, params = _build_trace_filter_clause(
        service=service,
        trace_id=trace_id,
        status=status,
        has_errors=has_errors,
        start_time_gte=start_time_gte,
        start_time_lte=start_time_lte,
        duration_min_ms=duration_min_ms,
        duration_max_ms=duration_max_ms,
    )

    with engine.connect() as conn:
        total_row = conn.execute(
            text(
                f"""
                SELECT COUNT(*) as total_count
                FROM {traces_table}
                WHERE {where}
                """
            ),
            params,
        ).fetchone()
        total_count = int(total_row[0]) if total_row else 0

        service_rows = conn.execute(
            text(
                f"""
                SELECT service_name, COUNT(*) as count
                FROM {traces_table}
                WHERE {where}
                GROUP BY service_name
                """
            ),
            params,
        ).fetchall()
        service_counts = {row[0]: int(row[1]) for row in service_rows if row[0] is not None}

        status_rows = conn.execute(
            text(
                f"""
                SELECT status, COUNT(*) as count
                FROM {traces_table}
                WHERE {where}
                GROUP BY status
                """
            ),
            params,
        ).fetchall()
        status_counts = {row[0].lower(): int(row[1]) for row in status_rows if row[0] is not None}

        error_rows = conn.execute(
            text(
                f"""
                SELECT
                    SUM(CASE WHEN error_count > 0 THEN 1 ELSE 0 END) as has_errors,
                    SUM(CASE WHEN error_count = 0 THEN 1 ELSE 0 END) as no_errors
                FROM {traces_table}
                WHERE {where}
                """
            ),
            params,
        ).fetchone()
        error_counts = {
            "has_errors": int(error_rows[0] or 0),
            "no_errors": int(error_rows[1] or 0),
        }

        duration_rows = conn.execute(
            text(
                f"""
                SELECT duration_ms
                FROM {traces_table}
                WHERE {where}
                """
            ),
            params,
        ).fetchall()
        durations = [int(row[0]) for row in duration_rows if row[0] is not None]

    histogram = _compute_histogram_bins(durations, bin_count=bin_count)
    percentiles = _compute_percentiles(durations)

    return {
        "total_count": total_count,
        "facet_counts": {
            "service": service_counts,
            "status": status_counts,
            "error": error_counts,
        },
        "duration_histogram": histogram,
        "percentiles": percentiles,
        "sampling": {"is_sampled": False, "sample_rate": 1.0},
        "truncation": {"is_truncated": False, "limit": None},
    }


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


def _to_ms(value: datetime) -> int | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return int(value.timestamp() * 1000)
    try:
        return int(datetime.fromisoformat(value).timestamp() * 1000)
    except Exception:
        return None


def _compute_self_time_map(spans: list[dict]) -> dict[str, int]:
    by_id = {span["span_id"]: span for span in spans if span.get("span_id")}
    child_intervals: dict[str, list[tuple[int, int]]] = {}

    for span in spans:
        parent_id = span.get("parent_span_id")
        if not parent_id or parent_id not in by_id:
            continue
        start_ms = _to_ms(span.get("start_time"))
        end_ms = _to_ms(span.get("end_time"))
        if start_ms is None or end_ms is None or end_ms <= start_ms:
            continue
        child_intervals.setdefault(parent_id, []).append((start_ms, end_ms))

    self_time = {}
    for span_id, span in by_id.items():
        duration = span.get("duration_ms")
        if duration is None:
            self_time[span_id] = None
            continue
        intervals = child_intervals.get(span_id, [])
        if not intervals:
            self_time[span_id] = int(duration)
            continue
        intervals.sort(key=lambda item: item[0])
        merged = []
        current_start, current_end = intervals[0]
        for start, end in intervals[1:]:
            if start <= current_end:
                current_end = max(current_end, end)
            else:
                merged.append((current_start, current_end))
                current_start, current_end = start, end
        merged.append((current_start, current_end))
        child_total = sum(end - start for start, end in merged)
        self_time[span_id] = max(0, int(duration) - int(child_total))

    return self_time


def _load_self_time_map(conn, trace_id: str, max_spans: int = 5000) -> dict[str, int]:
    spans_table = get_table_name("spans")
    rows = conn.execute(
        text(
            f"""
            SELECT span_id, parent_span_id, start_time, end_time, duration_ms
            FROM {spans_table}
            WHERE trace_id = :trace_id
            ORDER BY start_time ASC
            """
        ),
        {"trace_id": trace_id},
    ).fetchall()
    spans = [dict(row._mapping) for row in rows]
    if len(spans) > max_spans:
        return {}
    return _compute_self_time_map(spans)


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
        self_time_map = _load_self_time_map(conn, trace_id)
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
            data["self_time_ms"] = self_time_map.get(data["span_id"])
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
        self_time_map = _load_self_time_map(conn, trace_id)
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
        data["self_time_ms"] = self_time_map.get(data["span_id"])

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


def enqueue_ingestion_direct(payload_json: dict, trace_id: str = None) -> int:
    """Write the raw OTLP payload directly to the DB queue table."""
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


def enqueue_ingestion(payload_json: dict, trace_id: str = None) -> int:
    """Public entry point for enqueuing traces."""
    return safe_queue.enqueue(payload_json, trace_id)


def poll_ingestion_queue(limit: int = 10) -> list[dict]:
    """Fetch pending items from the ingestion queue and mark them as processing."""
    queue_table = get_table_name("ingestion_queue")
    # Atomic claim: update status to 'processing' and return the rows
    with engine.begin() as conn:
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


def get_metrics_preview(window_minutes: int, service: str = None):
    """Aggregate metrics over a time window for preview."""
    from datetime import timedelta

    traces_table = get_table_name("traces")
    start_time = datetime.now(timezone.utc) - timedelta(minutes=window_minutes)

    # Base params
    params = {"start_time": start_time}
    if service:
        params["service"] = service

    # Summary query
    # Percentile is Postgres specific, fallback for SQLite
    if engine.dialect.name == "postgresql":
        p95_expr = "PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY duration_ms)"
    else:
        p95_expr = "AVG(duration_ms)"  # Fallback

    summary_query = f"""
        SELECT
            COUNT(*) as total_count,
            COUNT(*) FILTER (WHERE status = 'ERROR') as error_count,
            AVG(duration_ms) as avg_duration,
            {p95_expr} as p95_duration
        FROM {traces_table}
        WHERE start_time >= :start_time
    """
    if service:
        summary_query += " AND service_name = :service"

    # Timeseries query
    # date_trunc is Postgres specific, fallback for SQLite
    if engine.dialect.name == "postgresql":
        interval = "1 minute" if window_minutes <= 60 else "1 hour"
        bucket_expr = f"date_trunc('{interval.split()[1]}', start_time)"
    else:
        bucket_expr = "datetime(strftime('%Y-%m-%dT%H:%M:00', start_time))"

    ts_query = f"""
        SELECT
            {bucket_expr} as timestamp,
            COUNT(*) as count,
            COUNT(*) FILTER (WHERE status = 'ERROR') as error_count,
            AVG(duration_ms) as avg_duration
        FROM {traces_table}
        WHERE start_time >= :start_time
    """
    if service:
        ts_query += " AND service_name = :service"
    ts_query += " GROUP BY 1 ORDER BY 1 ASC"

    with engine.connect() as conn:
        s_row = conn.execute(text(summary_query), params).fetchone()
        summary = (
            dict(s_row._mapping)
            if s_row
            else {"total_count": 0, "error_count": 0, "avg_duration": 0, "p95_duration": 0}
        )

        ts_rows = conn.execute(text(ts_query), params).fetchall()
        timeseries = [dict(r._mapping) for r in ts_rows]

        return {
            "summary": summary,
            "timeseries": timeseries,
            "window_minutes": window_minutes,
            "start_time": start_time,
        }
