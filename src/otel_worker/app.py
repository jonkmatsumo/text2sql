import asyncio
import base64
import gzip
import logging
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Optional

from fastapi import FastAPI, HTTPException, Query, Request, Response, status
from fastapi.middleware.cors import CORSMiddleware

from otel_worker.ingestion.limiter import limiter
from otel_worker.ingestion.monitor import OverflowAction, monitor
from otel_worker.ingestion.processor import coordinator
from otel_worker.logging import log_event
from otel_worker.metrics.coordinator import aggregation_coordinator, regression_coordinator
from otel_worker.models.api import (
    MetricsPreviewResponse,
    PaginatedSpansResponse,
    PaginatedTracesResponse,
    SpanDetail,
    TraceDetail,
)
from otel_worker.otlp.parser import (
    extract_trace_summaries,
    parse_otlp_json_traces,
    parse_otlp_traces,
)
from otel_worker.storage.minio import get_blob_by_url, get_trace_blob, init_minio
from otel_worker.storage.postgres import (
    enqueue_ingestion,
    get_metrics_preview,
    get_span_detail,
    get_trace,
    init_db,
    list_spans_for_trace,
    list_traces,
    resolve_trace_id_by_interaction,
    safe_queue,
)
from otel_worker.storage.reconciliation import reconciliation_coordinator

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifecycle manager for storage and background workers."""
    try:
        init_db()
        init_minio()
    except Exception as e:
        logger.error(f"Failed to initialize storage: {e}")

    safe_queue.start()
    await monitor.start()
    await coordinator.start()
    await aggregation_coordinator.start()
    await regression_coordinator.start()
    await reconciliation_coordinator.start()
    yield
    await reconciliation_coordinator.stop()
    await regression_coordinator.stop()
    await aggregation_coordinator.stop()
    await coordinator.stop()
    await monitor.stop()
    safe_queue.stop()


app = FastAPI(title="OTEL Dual-Write Worker", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "http://127.0.0.1:5173",
        "http://localhost:3000",
        "http://127.0.0.1:3000",
        "http://localhost:8501",
        "http://127.0.0.1:8501",
        "http://localhost:8080",
        "http://127.0.0.1:8080",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# --- Query API ---


@app.get("/api/v1/metrics/preview", response_model=MetricsPreviewResponse)
async def api_get_metrics_preview(
    window: str = Query("1h", regex="^(15m|1h|24h|7d)$"),
    service: Optional[str] = Query(None),
):
    """Fetch server-side aggregated metrics for preview."""
    # Parse window to minutes
    unit = window[-1]
    value = int(window[:-1])
    if unit == "m":
        minutes = value
    elif unit == "h":
        minutes = value * 60
    elif unit == "d":
        minutes = value * 24 * 60
    else:
        raise HTTPException(status_code=400, detail="Invalid window format")

    try:
        data = get_metrics_preview(window_minutes=minutes, service=service)
        return data
    except Exception as e:
        logger.error(f"Error computing metrics preview: {e}")
        raise HTTPException(status_code=500, detail="Failed to compute metrics")


@app.get("/api/v1/traces", response_model=PaginatedTracesResponse)
async def api_list_traces(
    service: Optional[str] = Query(None, description="Filter by service name"),
    trace_id: Optional[str] = Query(None, description="Exact trace ID lookup"),
    start_time_gte: Optional[datetime] = Query(
        None, description="Start time greater than or equal to"
    ),
    start_time_lte: Optional[datetime] = Query(
        None, description="Start time less than or equal to"
    ),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    order: str = Query("desc", regex="^(asc|desc)$"),
):
    """List ingested traces with filtering and pagination."""
    try:
        traces = list_traces(
            service=service,
            trace_id=trace_id,
            start_time_gte=start_time_gte,
            start_time_lte=start_time_lte,
            limit=limit,
            offset=offset,
            order=order,
        )
        # For 'total', we'd normally do a separate count(*) if offset is 0
        # or just return the items for now to keep it cheap.
        return PaginatedTracesResponse(
            items=traces,
            total=len(traces),  # Simplified
            next_offset=offset + limit if len(traces) == limit else None,
        )
    except Exception as e:
        logger.error(f"Error listing traces: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch traces")


@app.get("/api/v1/traces/{trace_id}", response_model=TraceDetail)
async def api_get_trace(
    trace_id: str,
    include: Optional[str] = Query(
        None, description="Comma separated fields to include (e.g., 'attributes')"
    ),
):
    """Get summarized details for a single trace."""
    include_attributes = False
    if include and "attributes" in include.split(","):
        include_attributes = True

    trace = get_trace(trace_id, include_attributes=include_attributes)
    if not trace:
        raise HTTPException(status_code=404, detail="Trace not found")
    return trace


@app.get("/api/v1/traces/by-interaction/{interaction_id}")
async def api_resolve_trace(interaction_id: str):
    """Resolve trace_id by interaction_id."""
    trace_id = resolve_trace_id_by_interaction(interaction_id)
    if not trace_id:
        raise HTTPException(status_code=404, detail="Trace not found for interaction")
    return {"trace_id": trace_id}


@app.get("/api/v1/traces/{trace_id}/spans", response_model=PaginatedSpansResponse)
async def api_list_spans(
    trace_id: str,
    limit: int = Query(200, ge=1, le=500),
    offset: int = Query(0, ge=0),
    include: Optional[str] = Query(
        None, description="Comma separated fields to include (e.g., 'attributes')"
    ),
):
    """List all spans associated with a trace."""
    include_attributes = False
    if include and "attributes" in include.split(","):
        include_attributes = True

    try:
        spans = list_spans_for_trace(
            trace_id, limit=limit, offset=offset, include_attributes=include_attributes
        )
        return PaginatedSpansResponse(
            items=spans,
            total=len(spans),
            next_offset=offset + limit if len(spans) == limit else None,
        )
    except Exception as e:
        logger.error(f"Error listing spans for trace {trace_id}: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch spans")


@app.get("/api/v1/traces/{trace_id}/spans/{span_id}", response_model=SpanDetail)
async def api_get_span_detail(trace_id: str, span_id: str):
    """Fetch detailed information for a single span."""
    span = get_span_detail(trace_id, span_id)
    if not span:
        raise HTTPException(status_code=404, detail="Span not found")
    return span


@app.get("/api/v1/traces/{trace_id}/raw")
async def api_get_raw_trace(trace_id: str):
    """Fetch raw OTLP blob from MinIO if available."""
    # We need to know the service name or have the full blob key.
    # Postgres stores raw_blob_url.
    trace = get_trace(trace_id)
    if not trace or not trace.get("raw_blob_url"):
        raise HTTPException(
            status_code=404, detail="Raw blob not found or available for this trace"
        )

    try:
        blob_data = None
        if isinstance(trace.get("raw_blob_url"), str) and trace["raw_blob_url"].startswith("s3://"):
            blob_data = get_blob_by_url(trace["raw_blob_url"])

        if not blob_data:
            blob_data = get_trace_blob(trace_id, trace["service_name"])
        if not blob_data:
            raise HTTPException(status_code=404, detail="Raw blob not found in storage")
        return Response(content=blob_data, media_type="application/json")
    except Exception as e:
        logger.error(f"Error fetching raw blob for trace {trace_id}: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch raw trace blob")


@app.get("/healthz")
async def healthz():
    """Health check endpoint."""
    return {"status": "ok"}


@app.post("/v1/traces")
async def receive_traces(request: Request):
    """Endpoint for OTLP traces (supports Protobuf and JSON)."""
    # Enforce Rate Limiting (Token Bucket)
    if not limiter.acquire():
        log_event("rate_limited", reason="limit_exceeded")
        return Response(
            content="Too Many Requests: Rate limit exceeded",
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
        )

    # Enforce queue overflow policy
    action = monitor.check_admissibility()
    if action == OverflowAction.REJECT:
        log_event(
            "queue_saturated",
            reason="queue_full_reject",
            action="reject",
            queue_depth=monitor._current_depth,
        )
        return Response(
            content="Too Many Requests: Ingestion queue full",
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
        )
    elif action == OverflowAction.DROP:
        log_event(
            "load_shedding",
            reason="queue_full_drop_or_sample",
            action="drop",
            queue_depth=monitor._current_depth,
        )
        # Return success to client but discard
        return Response(status_code=status.HTTP_202_ACCEPTED)

    content_type = request.headers.get("content-type", "")

    # Normalize content-type
    base_content_type = content_type.split(";")[0].strip().lower()

    content_encoded = request.headers.get("content-encoding", "").lower()

    body = await request.body()
    if content_encoded == "gzip":
        try:
            body = gzip.decompress(body)
        except Exception as e:
            logger.error(f"Failed to decompress gzip body: {e}")
            return Response(
                content=f"Decompression error: {e}", status_code=status.HTTP_400_BAD_REQUEST
            )
    try:
        # Validate content-type
        if base_content_type not in ("application/x-protobuf", "application/json"):
            msg = f"Unsupported content-type: '{content_type}'"
            logger.warning(msg)
            return Response(content=msg, status_code=status.HTTP_415_UNSUPPORTED_MEDIA_TYPE)

        # Determine payload to store
        # In durable mode, we store the raw payload and its format metadata
        payload = {
            "body_b64": base64.b64encode(body).decode("utf-8"),
            "content_type": base_content_type,
        }

        # Determine a primary trace_id for indexing (optional, but helpful)
        trace_id = None
        try:
            if base_content_type == "application/x-protobuf":
                parsed = parse_otlp_traces(body)
            else:
                parsed = parse_otlp_json_traces(body)

            summaries = extract_trace_summaries(parsed)
            if summaries:
                # Use the first trace ID as a hint
                tid_b64 = summaries[0]["trace_id"]
                trace_id = base64.b64decode(tid_b64).hex()
        except Exception:
            # If parsing fails, we still store the raw body for later recovery/debugging
            pass

        # Write to durable ingestion queue (E)
        await asyncio.to_thread(enqueue_ingestion, payload, trace_id=trace_id)

        return Response(status_code=status.HTTP_202_ACCEPTED)
    except Exception as e:
        logger.error(f"Internal error enqueuing traces: {e}")
        return Response(
            content=f"Internal Server Error: {e}",
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )
