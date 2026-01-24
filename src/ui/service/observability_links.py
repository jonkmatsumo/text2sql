"""Observability deep-link utilities.

Provides URL builders for Grafana dashboards and OTEL Worker API endpoints.
"""

import os

# Environment variable defaults for local development
GRAFANA_BASE_URL = os.getenv("GRAFANA_BASE_URL", "http://localhost:3001")
OTEL_WORKER_BASE_URL = os.getenv("OTEL_WORKER_BASE_URL", "http://localhost:4320")


def grafana_trace_detail_url(trace_id: str) -> str:
    """Build URL to Grafana trace detail dashboard.

    Args:
        trace_id: The trace ID to view.

    Returns:
        Full URL to the trace detail dashboard with trace_id variable set.
    """
    return f"{GRAFANA_BASE_URL}/d/text2sql-trace-detail?var-trace_id={trace_id}"


def otel_trace_url(trace_id: str) -> str:
    """Build URL to OTEL Worker trace summary endpoint.

    Args:
        trace_id: The trace ID to query.

    Returns:
        Full URL to the trace API endpoint.
    """
    return f"{OTEL_WORKER_BASE_URL}/api/v1/traces/{trace_id}"


def otel_spans_url(trace_id: str) -> str:
    """Build URL to OTEL Worker spans endpoint with attributes.

    Args:
        trace_id: The trace ID to query.

    Returns:
        Full URL to the spans API endpoint with include=attributes.
    """
    return f"{OTEL_WORKER_BASE_URL}/api/v1/traces/{trace_id}/spans?include=attributes"


def otel_raw_url(trace_id: str) -> str:
    """Build URL to OTEL Worker raw OTLP blob endpoint.

    Args:
        trace_id: The trace ID to query.

    Returns:
        Full URL to the raw OTLP endpoint.
    """
    return f"{OTEL_WORKER_BASE_URL}/api/v1/traces/{trace_id}/raw"
