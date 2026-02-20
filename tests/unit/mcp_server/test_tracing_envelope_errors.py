"""Tests for envelope-level MCP tracing error status handling."""

from unittest.mock import patch

import pytest
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import StatusCode

from mcp_server.utils.tracing import trace_tool


def _in_memory_tracer():
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    return provider.get_tracer("mcp.server"), exporter


@pytest.mark.asyncio
async def test_trace_tool_marks_error_for_error_envelope():
    """Envelope responses with non-null error should mark span status as ERROR."""
    tracer, exporter = _in_memory_tracer()

    async def handler():
        return {
            "result": None,
            "metadata": {"provider": "postgres"},
            "error": {
                "category": "invalid_request",
                "message": "Validation failed",
                "retryable": False,
            },
        }

    with patch("opentelemetry.trace.get_tracer", return_value=tracer):
        traced = trace_tool("list_tables")(handler)
        await traced()

    spans = exporter.get_finished_spans()
    assert len(spans) == 1
    span = spans[0]
    assert span.status.status_code == StatusCode.ERROR
    assert span.attributes["mcp.tool.error.category"] == "invalid_request"


@pytest.mark.asyncio
async def test_trace_tool_keeps_ok_for_non_error_envelope():
    """Envelope responses with error=None should keep span status OK."""
    tracer, exporter = _in_memory_tracer()

    async def handler():
        return {
            "result": [{"id": 1}],
            "metadata": {"provider": "postgres"},
            "error": None,
        }

    with patch("opentelemetry.trace.get_tracer", return_value=tracer):
        traced = trace_tool("list_tables")(handler)
        await traced()

    spans = exporter.get_finished_spans()
    assert len(spans) == 1
    span = spans[0]
    assert span.status.status_code == StatusCode.OK
    assert "mcp.tool.error.category" not in span.attributes
