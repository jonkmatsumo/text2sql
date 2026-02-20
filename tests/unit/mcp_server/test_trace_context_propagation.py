"""Tests for trace context propagation into MCP tool spans."""

from unittest.mock import patch

import pytest
from opentelemetry import propagate
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from mcp_server.utils.tracing import trace_tool


def _in_memory_tracer():
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    return provider.get_tracer("mcp.server"), exporter


@pytest.mark.asyncio
async def test_trace_tool_uses_injected_parent_context_and_strips_reserved_key():
    """MCP span should parent to injected trace context and hide reserved kwargs from handlers."""
    tracer, exporter = _in_memory_tracer()
    captured_kwargs = {}

    async def handler(**kwargs):
        captured_kwargs.update(kwargs)
        return {"result": {"ok": True}, "metadata": {"provider": "postgres"}, "error": None}

    with tracer.start_as_current_span("agent.parent") as parent_span:
        carrier = {}
        propagate.inject(carrier)
        parent_span_id = parent_span.get_span_context().span_id
        parent_trace_id = parent_span.get_span_context().trace_id

    with patch("opentelemetry.trace.get_tracer", return_value=tracer):
        traced = trace_tool("list_tables")(handler)
        await traced(_trace_context=carrier, tenant_id=13)

    spans = exporter.get_finished_spans()
    child = next(span for span in spans if span.name == "mcp.tool.list_tables")

    assert child.parent is not None
    assert child.parent.span_id == parent_span_id
    assert child.context.trace_id == parent_trace_id
    assert "_trace_context" not in captured_kwargs
    assert captured_kwargs["tenant_id"] == 13
