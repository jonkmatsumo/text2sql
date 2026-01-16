import logging
from typing import Any, Dict, List, Optional

from sqlalchemy import text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)

# Constants for stage mapping
STAGE_PATTERNS = {
    "router": ["router_node"],
    "retrieval": ["retrieve_context_node"],
    "generation": ["generate_sql_node"],
    "execution": ["validate_and_execute_node"],
    "synthesis": ["synthesize_insight_node"],
}


def get_stage_from_span_name(span_name: str) -> Optional[str]:
    """Map a span name to a logical stage."""
    for stage, patterns in STAGE_PATTERNS.items():
        if span_name in patterns:
            return stage
    return None


def compute_trace_metrics(trace_row: Dict[str, Any]) -> Dict[str, Any]:
    """Compute derived metrics for a single trace."""
    # Trace row from otel.traces already has duration_ms and status
    # We essentially project it to the metrics table format
    has_error = trace_row.get("status") == "ERROR" or trace_row.get("error_count", 0) > 0
    return {
        "trace_id": trace_row["trace_id"],
        "service_name": trace_row.get("service_name"),
        "start_time": trace_row.get("start_time"),
        "end_time": trace_row.get("end_time"),
        "duration_ms": trace_row.get("duration_ms"),
        "has_error": has_error,
    }


def compute_stage_metrics(spans: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Compute metrics for each stage found in the spans."""
    stage_metrics = []

    stage_durations: Dict[str, int] = {}
    stage_errors: Dict[str, bool] = {}

    for span in spans:
        stage = get_stage_from_span_name(span["name"])
        if not stage:
            continue

        dur = span.get("duration_ms", 0) or 0
        stage_durations[stage] = stage_durations.get(stage, 0) + dur

        # Check error status
        is_error = span.get("status_code") == "STATUS_CODE_ERROR"
        if is_error:
            stage_errors[stage] = True

    trace_id = spans[0]["trace_id"] if spans else None
    if not trace_id:
        return []

    for stage, duration in stage_durations.items():
        stage_metrics.append(
            {
                "trace_id": trace_id,
                "stage": stage,
                "duration_ms": duration,
                "has_error": stage_errors.get(stage, False),
            }
        )

    return stage_metrics


def run_aggregation(
    engine: Engine, lookback_minutes: int = 60, batch_size: int = 100
) -> Dict[str, int]:
    """Run the aggregation job to populate metrics tables."""
    # 1. Select traces updated recently that haven't been aggregated or need re-aggregation.
    # For simplicity and idempotency, we just re-process traces from the last N minutes.
    stats = {"traces_processed": 0, "stage_metrics_rows": 0}

    with engine.connect() as conn:
        # Fetch traces
        query = text(
            """
            SELECT * FROM otel.traces
            WHERE start_time >= NOW() - INTERVAL ':mins minutes'
            ORDER BY start_time ASC
        """.replace(
                ":mins", str(lookback_minutes)
            )
        )

        result = conn.execute(query)
        traces = [dict(row._mapping) for row in result]

    if not traces:
        logger.info("No traces found to aggregate.")
        return stats

    logger.info(f"Aggregating metrics for {len(traces)} traces...")

    with engine.begin() as conn:
        for trace in traces:
            trace_id = trace["trace_id"]

            # 1. Compute and Upsert Trace Metrics
            t_metrics = compute_trace_metrics(trace)
            conn.execute(
                text(
                    """
                    INSERT INTO otel.trace_metrics (
                        trace_id, service_name, start_time, end_time, duration_ms, has_error
                    ) VALUES (
                        :trace_id, :service_name, :start_time, :end_time, :duration_ms, :has_error
                    )
                    ON CONFLICT (trace_id) DO UPDATE SET
                        end_time = EXCLUDED.end_time,
                        duration_ms = EXCLUDED.duration_ms,
                        has_error = EXCLUDED.has_error;
                """
                ),
                t_metrics,
            )
            stats["traces_processed"] += 1

            # 2. Fetch Spans for Stage Metrics
            spans_result = conn.execute(
                text("SELECT * FROM otel.spans WHERE trace_id = :tid"),
                {"tid": trace_id},
            )
            spans = [dict(row._mapping) for row in spans_result]

            # 3. Compute Stage Metrics
            s_metrics = compute_stage_metrics(spans)

            # Clean up old stage metrics for this trace
            conn.execute(
                text("DELETE FROM otel.stage_metrics WHERE trace_id = :tid"),
                {"tid": trace_id},
            )

            if s_metrics:
                for sm in s_metrics:
                    conn.execute(
                        text(
                            """
                            INSERT INTO otel.stage_metrics (trace_id, stage, duration_ms, has_error)
                            VALUES (:trace_id, :stage, :duration_ms, :has_error)
                        """
                        ),
                        sm,
                    )
                stats["stage_metrics_rows"] += len(s_metrics)

    logger.info(f"Aggregation complete. Stats: {stats}")
    return stats
