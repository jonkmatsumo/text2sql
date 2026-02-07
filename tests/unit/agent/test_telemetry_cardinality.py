"""Tests for telemetry cardinality and safety guardrails."""

import json
from unittest.mock import patch

import pytest
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from agent.telemetry import OTELTelemetryBackend, TelemetryService


@pytest.fixture
def otel_service():
    """Fixture to provide an in-memory OTEL telemetry service for testing."""
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))

    with patch("opentelemetry.trace.get_tracer") as mock_get_tracer:
        mock_get_tracer.side_effect = lambda name: provider.get_tracer(name)
        backend = OTELTelemetryBackend()
        service = TelemetryService(backend=backend)
        yield service, exporter


def test_telemetry_bounds_long_strings(otel_service):
    """Test that long string attributes are truncated."""
    service, exporter = otel_service

    long_string = "a" * 3000
    with service.start_span("test", attributes={"long_attr": long_string}):
        pass

    span = exporter.get_finished_spans()[0]
    val = span.attributes["long_attr"]
    assert len(val) <= 2048
    assert val.endswith("...")


def test_telemetry_hashes_sql(otel_service):
    """Test that SQL-related attributes are hashed and summarized."""
    service, exporter = otel_service

    sql = "SELECT * FROM users WHERE id = 1"
    with service.start_span("test", attributes={"current_sql": sql}):
        pass

    span = exporter.get_finished_spans()[0]
    val = span.attributes["current_sql"]
    assert val.startswith("hash:")
    assert "SELECT * FROM users" in val


def test_telemetry_bounds_high_cardinality_collections(otel_service):
    """Test that high-cardinality collections are bounded and stringified."""
    service, exporter = otel_service

    # Large list of dicts
    large_result = [{"id": i, "data": "val"} for i in range(100)]

    with service.start_span("test", attributes={"query_result": large_result}):
        pass

    span = exporter.get_finished_spans()[0]
    val = span.attributes["query_result"]

    # Should be a JSON string (because it's high cardinality)
    data = json.loads(val)
    # collections are bounded to 20 items in bound_attribute
    assert len(data) == 20
    # Metadata should be present from bound_payload
    assert data[0] == {"id": 0, "data": "val"}


def test_telemetry_redacts_sensitive_keys(otel_service):
    """Test that sensitive keys are redacted from attributes."""
    service, exporter = otel_service

    with service.start_span("test", attributes={"api_key": "sk-12345", "other": "val"}):
        pass

    span = exporter.get_finished_spans()[0]
    assert span.attributes["api_key"] == "<redacted>"
    assert span.attributes["other"] == "val"
