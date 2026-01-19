#!/usr/bin/env python3
"""Verification script for trace metrics aggregation."""

import os
import sys

# Add paths
sys.path.append(os.path.join(os.getcwd(), "observability/otel-worker/src"))
sys.path.append(os.path.join(os.getcwd(), "agent/src"))

from otel_worker.metrics.aggregate import compute_trace_metrics  # noqa: E402


def test_compute_trace_metrics():
    """Test basic trace functionality."""
    print("Testing compute_trace_metrics...")
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
    assert metrics["trace_id"] == "t1", "trace_id mismatch"
    assert metrics["total_tokens"] == 0
    print("PASS: compute_trace_metrics basic")


def test_compute_trace_metrics_with_tokens():
    """Test token aggregation logic."""
    print("Testing compute_trace_metrics_with_tokens...")
    trace_row = {
        "trace_id": "t1",
        "service_name": "agent",
        "status": "OK",
    }
    spans = [
        {
            "span_id": "s1",
            "name": "node1",
            "span_attributes": {
                "llm.token_usage.input_tokens": 10,
                "llm.token_usage.output_tokens": 20,
                "llm.token_usage.total_tokens": 30,
            },
        },
        {
            "span_id": "s2",
            "name": "node2",
            "span_attributes": {
                "llm.token_usage.input_tokens": 5,
                "llm.token_usage.output_tokens": 5,
            },
        },
    ]

    metrics = compute_trace_metrics(trace_row, spans)
    assert (
        metrics["prompt_tokens"] == 15
    ), f"Expected 15 prompt tokens, got {metrics['prompt_tokens']}"
    assert metrics["completion_tokens"] == 25
    assert metrics["total_tokens"] == 40
    print("PASS: compute_trace_metrics_with_tokens")


if __name__ == "__main__":
    try:
        test_compute_trace_metrics()
        test_compute_trace_metrics_with_tokens()
        print("All tests passed!")
    except AssertionError as e:
        print(f"FAIL: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"ERROR: {e}")
        sys.exit(1)
