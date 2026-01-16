from otel_worker.metrics.aggregate import (
    compute_stage_metrics,
    compute_trace_metrics,
    get_stage_from_span_name,
)


class TestMetricsAggregation:
    """Test suite for metrics aggregation logic."""

    def test_get_stage_from_span_name(self):
        """Verify span name to stage mapping."""
        assert get_stage_from_span_name("router_node") == "router"
        assert get_stage_from_span_name("generate_sql_node") == "generation"
        assert get_stage_from_span_name("unknown_node") is None

    def test_compute_trace_metrics(self):
        """Verify trace metrics computation."""
        trace_row = {
            "trace_id": "t1",
            "service_name": "agent",
            "start_time": "2023-01-01T10:00:00Z",
            "end_time": "2023-01-01T10:00:01Z",
            "duration_ms": 1000,
            "status": "OK",
            "error_count": 0,
        }
        metrics = compute_trace_metrics(trace_row)
        assert metrics["trace_id"] == "t1"
        assert metrics["has_error"] is False
        assert metrics["duration_ms"] == 1000

        # Test error case
        trace_row_err = trace_row.copy()
        trace_row_err["status"] = "ERROR"
        metrics_err = compute_trace_metrics(trace_row_err)
        assert metrics_err["has_error"] is True

    def test_compute_stage_metrics(self):
        """Verify stage metrics computation."""
        spans = [
            # Router stage
            {
                "trace_id": "t1",
                "name": "router_node",
                "duration_ms": 100,
                "status_code": "OK",
            },
            # Generation stage (failed)
            {
                "trace_id": "t1",
                "name": "generate_sql_node",
                "duration_ms": 500,
                "status_code": "STATUS_CODE_ERROR",
            },
            # Another generation span (retry)
            {
                "trace_id": "t1",
                "name": "generate_sql_node",
                "duration_ms": 400,
                "status_code": "OK",
            },
            # Irrelevant span
            {
                "trace_id": "t1",
                "name": "some_internal_helper",
                "duration_ms": 50,
                "status_code": "OK",
            },
        ]

        metrics = compute_stage_metrics(spans)

        # Expect 2 stages: router, generation
        assert len(metrics) == 2

        # Verify Router
        router = next(m for m in metrics if m["stage"] == "router")
        assert router["duration_ms"] == 100
        assert router["has_error"] is False

        # Verify Generation (accumulation of duration, error flag set if any span failed)
        generation = next(m for m in metrics if m["stage"] == "generation")
        assert generation["duration_ms"] == 900  # 500 + 400
        assert generation["has_error"] is True

    def test_compute_stage_metrics_empty(self):
        """Verify empty span list handling."""
        assert compute_stage_metrics([]) == []
