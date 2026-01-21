import datetime
import logging
import math
import uuid
from datetime import timedelta, timezone
from typing import List

from sqlalchemy import text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)


def calculate_percentile(values: List[float], percentile: float) -> float:
    """Calculate the p-th percentile from a list of values."""
    if not values:
        return 0.0
    sorted_values = sorted(values)
    k = (len(sorted_values) - 1) * (percentile / 100.0)
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return sorted_values[int(k)]
    d0 = sorted_values[int(f)] * (c - k)
    d1 = sorted_values[int(c)] * (k - f)
    return d0 + d1


def compute_regressions(
    engine: Engine,
    candidate_minutes: int = 30,
    baseline_minutes: int = 30,
    offset_minutes: int = 30,
    min_samples: int = 10,
    threshold_pct_latency: float = 20.0,
    threshold_abs_latency_ms: float = 200.0,
    threshold_pct_error: float = 10.0,  # e.g. 10% increase in error rate
) -> int:
    """
    Compute regressions between candidate and baseline windows.

    Time windows:
    - Candidate: [now - candidate_minutes, now]
    - Baseline: [now - candidate_minutes - offset_minutes - baseline_minutes,
                 now - candidate_minutes - offset_minutes]
      (Wait, typically baseline is 'previous N minutes'.
       Let's say now is 12:00. candidate=30, offset=0. Candidate: 11:30-12:00.
       If offset=0, baseline ends at 11:30. Baseline=30. Baseline: 11:00-11:30.
       If offset=1440 (24h), baseline ends at 11:30 yesterday.

       Baseline end = candidate start - offset.
       Baseline start = Baseline end - baseline_minutes.
    )
    """
    now = datetime.datetime.now(timezone.utc)

    candidate_end = now
    candidate_start = now - timedelta(minutes=candidate_minutes)

    baseline_end = candidate_start - timedelta(minutes=offset_minutes)
    baseline_start = baseline_end - timedelta(minutes=baseline_minutes)

    logger.info(
        f"Computing regressions. Candidate: [{candidate_start}, {candidate_end}]. "
        f"Baseline: [{baseline_start}, {baseline_end}]"
    )

    # Metrics to check
    # 1. Latency P95 (duration_ms)
    # 2. Error Rate (has_error)
    # 3. Token Usage (total_tokens) - specific check? Maybe just Average or P95.

    # We will compute basic aggregates in Memory for simplicity, assuming data volume allows.
    # If volume is huge, we should push aggregation to SQL.
    # Given 'otel.trace_metrics' is one row per trace, retrieving all rows for a 30m window
    # might be okay (thousands).
    # If millions, use SQL aggregation.

    # Let's use SQL aggregation for efficiency.

    regressions_found = 0

    with engine.begin() as conn:
        # Helper to get stats for a window
        def get_stats(start, end):
            query = text(
                """
                SELECT
                    COUNT(*) as count,
                    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY duration_ms) as lat_p50,
                    PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY duration_ms) as lat_p90,
                    PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY duration_ms) as lat_p99,
                    SUM(CASE WHEN has_error THEN 1 ELSE 0 END)::float / NULLIF(COUNT(*), 0)
                    as error_rate,
                    AVG(total_tokens) as tokens_avg,
                    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY total_tokens) as tokens_p95
                FROM otel.trace_metrics
                WHERE start_time >= :start AND start_time < :end
            """
            )
            result = conn.execute(query, {"start": start, "end": end}).fetchone()
            return result

        cand_stats = get_stats(candidate_start, candidate_end)
        base_stats = get_stats(baseline_start, baseline_end)

        if not cand_stats or not base_stats:
            logger.warning("No data for regression check.")
            return 0

        cand_count = cand_stats.count
        base_count = base_stats.count

        if cand_count < min_samples or base_count < min_samples:
            logger.info(
                f"Skipping regression check due to low sample size. "
                f"Cand: {cand_count}, Base: {base_count}"
            )
            return 0

        # Define checks
        checks = [
            (
                "latency_p50",
                cand_stats.lat_p50,
                base_stats.lat_p50,
                threshold_pct_latency,
                threshold_abs_latency_ms,
            ),
            (
                "latency_p90",
                cand_stats.lat_p90,
                base_stats.lat_p90,
                threshold_pct_latency,
                threshold_abs_latency_ms,
            ),
            (
                "latency_p99",
                cand_stats.lat_p99,
                base_stats.lat_p99,
                threshold_pct_latency,
                threshold_abs_latency_ms,
            ),
            # Error rate is a ratio (0.0-1.0). Thresholds: pct means % increase of the rate
            # (relative), abs means raw diff (e.g. +0.05)
            # abs means raw diff (e.g. +0.05)
            # Threshold pct=10 means 10% increase. 0.1 -> 0.11.
            # Let's use threshold_pct_error and hardcode abs for now or use passed param.
            (
                "error_rate",
                cand_stats.error_rate or 0.0,
                base_stats.error_rate or 0.0,
                threshold_pct_error,
                0.05,
            ),  # 5% absolute increase in error rate allowed
            (
                "tokens_avg",
                cand_stats.tokens_avg or 0,
                base_stats.tokens_avg or 0,
                20.0,
                100.0,
            ),  # 20% or 100 tokens
        ]

        for name, cand_val, base_val, thr_pct, thr_abs in checks:
            cand_val = float(cand_val or 0)
            base_val = float(base_val or 0)

            delta_abs = cand_val - base_val
            delta_pct = 0.0
            if base_val > 0:
                delta_pct = (delta_abs / base_val) * 100.0

            status = "pass"

            # Check regression
            if delta_abs > 0:  # Only care about increases for "bad" metrics
                if delta_abs >= thr_abs and delta_pct >= thr_pct:
                    status = "fail"
                elif delta_abs >= (thr_abs * 0.8) and delta_pct >= (thr_pct * 0.8):
                    # Optional warning logic
                    status = "warn"

            if status != "pass":
                # Find exemplar trace IDs (top 5 worst for this metric)
                # Need a separate query.
                top_trace_ids = []
                if "latency" in name:
                    q = text(
                        """
                        SELECT trace_id FROM otel.trace_metrics
                        WHERE start_time >= :start AND start_time < :end
                        ORDER BY duration_ms DESC LIMIT 5
                    """
                    )
                    top_trace_ids = [
                        r[0]
                        for r in conn.execute(q, {"start": candidate_start, "end": candidate_end})
                    ]
                elif "error" in name:
                    q = text(
                        """
                        SELECT trace_id FROM otel.trace_metrics
                        WHERE start_time >= :start AND start_time < :end
                          AND has_error = true
                        LIMIT 5
                    """
                    )
                    top_trace_ids = [
                        r[0]
                        for r in conn.execute(q, {"start": candidate_start, "end": candidate_end})
                    ]

                # Persist regression
                reg_id = str(uuid.uuid4())
                conn.execute(
                    text(
                        """
                        INSERT INTO otel.metric_regressions (
                            id, computed_at, window_start, window_end,
                            baseline_window_start, baseline_window_end,
                            metric_name, baseline_value, candidate_value,
                            delta_abs, delta_pct, status, sample_size, top_trace_ids
                        ) VALUES (
                            :id, NOW(), :ws, :we, :bws, :bwe,
                            :name, :bv, :cv, :da, :dp, :status, :ss, :tids
                        )
                    """
                    ),
                    {
                        "id": reg_id,
                        "ws": candidate_start,
                        "we": candidate_end,
                        "bws": baseline_start,
                        "bwe": baseline_end,
                        "name": name,
                        "bv": base_val,
                        "cv": cand_val,
                        "da": delta_abs,
                        "dp": delta_pct,
                        "status": status,
                        "ss": cand_count,
                        "tids": top_trace_ids,
                        # SQLAlchemy handles list->array/json depending on driver,
                        # casting is safer if JSONB
                    },
                )
                logger.warning(
                    f"Regression detected: {name} "
                    f"(Delta: {delta_pct:.1f}%, Abs: {delta_abs:.2f}). Status: {status}"
                )
                regressions_found += 1

    return regressions_found
