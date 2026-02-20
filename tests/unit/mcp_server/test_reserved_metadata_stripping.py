"""Regression tests for canonical reserved metadata stripping in MCP tracing."""

from __future__ import annotations

from unittest.mock import patch

import pytest
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from mcp_server.utils.reserved_fields import (
    REQUEST_ID_RESERVED_FIELD,
    RESERVED_TOOL_METADATA_KEYS,
    TRACE_CONTEXT_RESERVED_FIELD,
)
from mcp_server.utils.tracing import trace_tool


def _in_memory_tracer():
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    return provider.get_tracer("mcp.server"), exporter


@pytest.mark.asyncio
async def test_trace_tool_strips_all_registered_reserved_metadata_keys():
    """Any key in the canonical reserved-key registry must never reach handlers."""
    tracer, _exporter = _in_memory_tracer()
    captured_kwargs: dict = {}

    async def handler(**kwargs):
        captured_kwargs.update(kwargs)
        return {"result": {"ok": True}, "metadata": {"provider": "postgres"}, "error": None}

    reserved_payload = {key: "reserved-value" for key in RESERVED_TOOL_METADATA_KEYS}
    reserved_payload[REQUEST_ID_RESERVED_FIELD] = "req-reserved-1"
    reserved_payload[TRACE_CONTEXT_RESERVED_FIELD] = {
        "traceparent": "00-80e1afed08e019fc1110464cfa66635c-7a085853722dc6d2-01"
    }

    with patch("opentelemetry.trace.get_tracer", return_value=tracer):
        traced = trace_tool("list_tables")(handler)
        response = await traced(query="orders", tenant_id=9, **reserved_payload)

    for key in RESERVED_TOOL_METADATA_KEYS:
        assert key not in captured_kwargs
    assert captured_kwargs["query"] == "orders"
    assert captured_kwargs["tenant_id"] == 9
    assert response["metadata"]["request_id"] == "req-reserved-1"
