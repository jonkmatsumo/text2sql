"""Tests for canonical error-code passthrough in MCP envelopes and telemetry."""

import json
from types import SimpleNamespace
from unittest.mock import patch

import pytest
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from mcp_server.tools.execute_sql_query import handler
from mcp_server.utils.tracing import trace_tool


def _in_memory_tracer():
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    return provider.get_tracer("mcp.server")


@pytest.mark.asyncio
async def test_trace_tool_uses_error_code_without_remapping():
    """Tracing metrics should preserve canonical error_code from the envelope."""

    async def _handler():
        return {
            "result": None,
            "metadata": {"provider": "postgres"},
            "error": {
                "category": "invalid_request",
                "code": "SOME_PROVIDER_CODE",
                "error_code": "READONLY_VIOLATION",
                "message": "blocked",
                "retryable": False,
            },
        }

    with (
        patch("opentelemetry.trace.get_tracer", return_value=_in_memory_tracer()),
        patch("mcp_server.utils.tracing.mcp_metrics.add_counter") as mock_add_counter,
    ):
        traced = trace_tool("list_tables")(_handler)
        await traced()

    logical_error_calls = [
        call
        for call in mock_add_counter.call_args_list
        if call.args and call.args[0] == "mcp.tool.logical_failures_total"
    ]
    assert len(logical_error_calls) == 1
    assert logical_error_calls[0].kwargs["attributes"]["error_code"] == "READONLY_VIOLATION"


@pytest.mark.asyncio
async def test_trace_tool_derives_canonical_error_code_from_category_when_missing():
    """Tracing should derive canonical code from category when error_code is absent."""

    async def _handler():
        return {
            "result": None,
            "metadata": {"provider": "postgres"},
            "error": {
                "category": "timeout",
                "code": "DRIVER_TIMEOUT_123",
                "message": "timed out",
                "retryable": True,
            },
        }

    with (
        patch("opentelemetry.trace.get_tracer", return_value=_in_memory_tracer()),
        patch("mcp_server.utils.tracing.mcp_metrics.add_counter") as mock_add_counter,
    ):
        traced = trace_tool("list_tables")(_handler)
        await traced()

    logical_error_calls = [
        call
        for call in mock_add_counter.call_args_list
        if call.args and call.args[0] == "mcp.tool.logical_failures_total"
    ]
    assert len(logical_error_calls) == 1
    assert logical_error_calls[0].kwargs["attributes"]["error_code"] == "DB_TIMEOUT"


@pytest.mark.asyncio
async def test_execute_sql_tenant_rejection_includes_canonical_error_code():
    """Tenant-enforcement rejections should surface canonical error_code in the envelope."""
    with (
        patch("mcp_server.utils.auth.validate_role", return_value=None),
        patch(
            "mcp_server.tools.execute_sql_query.Database.get_query_target_capabilities",
            return_value=SimpleNamespace(
                provider_name="sqlite",
                tenant_enforcement_mode="unsupported",
                supports_column_metadata=True,
                supports_cancel=True,
                supports_pagination=True,
                execution_model="sync",
            ),
        ),
    ):
        payload = await handler("SELECT 1", tenant_id=7)

    result = json.loads(payload)
    assert result["error"]["category"] == "TENANT_ENFORCEMENT_UNSUPPORTED"
    assert result["error"]["error_code"] == "TENANT_ENFORCEMENT_UNSUPPORTED"
