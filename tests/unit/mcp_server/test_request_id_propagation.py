"""Tests for MCP request_id propagation and envelope metadata injection."""

import json
from unittest.mock import patch

import pytest
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from common.observability.context import request_id_var
from mcp_server.utils.context import ToolContext
from mcp_server.utils.tracing import trace_tool


def _in_memory_tracer():
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    return provider.get_tracer("mcp.server"), exporter


@pytest.mark.asyncio
async def test_trace_tool_propagates_request_id_to_span_and_envelope():
    """Reserved _request_id should be removed from handler args and added to span/envelope."""
    tracer, exporter = _in_memory_tracer()
    captured_kwargs = {}

    async def handler(**kwargs):
        captured_kwargs.update(kwargs)
        return {"result": {"ok": True}, "metadata": {"provider": "postgres"}, "error": None}

    with patch("opentelemetry.trace.get_tracer", return_value=tracer):
        traced = trace_tool("list_tables")(handler)
        response = await traced(_request_id="req-abc-123", tenant_id=17)

    assert "_request_id" not in captured_kwargs
    assert response["metadata"]["request_id"] == "req-abc-123"

    spans = exporter.get_finished_spans()
    span = next(finished for finished in spans if finished.name == "mcp.tool.list_tables")
    expected_trace_id = format(span.context.trace_id, "032x")
    assert span.attributes["mcp.request_id"] == "req-abc-123"
    assert span.attributes["mcp.trace_id"] == expected_trace_id
    assert response["metadata"]["trace_id"] == expected_trace_id


@pytest.mark.asyncio
async def test_trace_tool_injects_request_id_for_json_string_envelopes():
    """JSON-string envelopes should retain additive metadata.request_id injection."""
    tracer, exporter = _in_memory_tracer()

    async def handler():
        return json.dumps({"result": {"ok": True}, "metadata": {"provider": "postgres"}})

    with patch("opentelemetry.trace.get_tracer", return_value=tracer):
        traced = trace_tool("list_tables")(handler)
        response = await traced(_request_id="req-json-1")

    payload = json.loads(response)
    assert payload["metadata"]["request_id"] == "req-json-1"
    spans = exporter.get_finished_spans()
    span = next(finished for finished in spans if finished.name == "mcp.tool.list_tables")
    assert payload["metadata"]["trace_id"] == format(span.context.trace_id, "032x")


def test_tool_context_uses_request_id_from_request_scope():
    """Use request-scoped request_id values when building ToolContext."""
    token = request_id_var.set("ctx-req-42")
    try:
        context = ToolContext.from_env(tenant_id=5)
    finally:
        request_id_var.reset(token)

    assert context.request_id == "ctx-req-42"
