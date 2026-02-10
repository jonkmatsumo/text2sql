"""Regression tests for MCP tool observability parity.

Ensures that trace_tool emits the required span attributes for non-SQL
tools so observability gaps do not silently regress.
"""

import json
from unittest.mock import MagicMock, patch

import pytest

from mcp_server.utils.tracing import trace_tool


def _make_mock_tracer():
    """Create a mock tracer that captures span attribute calls."""
    mock_tracer = MagicMock()
    mock_span = MagicMock()
    mock_span.is_recording.return_value = True
    mock_tracer.start_as_current_span.return_value.__enter__ = MagicMock(return_value=mock_span)
    mock_tracer.start_as_current_span.return_value.__exit__ = MagicMock(return_value=False)
    return mock_tracer, mock_span


@pytest.mark.asyncio
async def test_non_sql_tool_emits_required_span_attributes():
    """Verify that a non-SQL tool emits tool.name, tenant_id, and truncation attributes."""
    mock_tracer, mock_span = _make_mock_tracer()

    # Simulate a tool handler that returns a ToolResponseEnvelope JSON
    envelope = {
        "schema_version": "1.0",
        "result": [{"id": 1, "name": "test"}],
        "metadata": {"provider": "postgres"},
    }

    async def fake_handler(query: str, tenant_id: int = None):
        return json.dumps(envelope)

    with patch("opentelemetry.trace.get_tracer", return_value=mock_tracer):
        traced = trace_tool("get_sample_data")(fake_handler)
        await traced(query="test", tenant_id=42)

    # Collect all set_attribute calls into a dict for easy assertion
    attrs = {}
    for call in mock_span.set_attribute.call_args_list:
        key, value = call[0]
        attrs[key] = value

    # Required attributes for every tool
    assert attrs["mcp.tool.name"] == "get_sample_data", "tool.name must be set"
    assert attrs["mcp.tenant_id"] == "42", "tenant_id must be recorded when present"
    assert "mcp.tool.response.size_bytes" in attrs, "response size must be recorded"
    assert "mcp.tool.response.truncated" in attrs, "truncation flag must be recorded"
    assert (
        "mcp.tool.response.truncation_parse_failed" in attrs
    ), "truncation parse status must be recorded"
    assert "mcp.tool.request.size_bytes" in attrs, "request size must be recorded"


@pytest.mark.asyncio
async def test_tool_without_tenant_id_omits_tenant_attribute():
    """Verify that tenant_id attribute is not set when tenant_id is absent."""
    mock_tracer, mock_span = _make_mock_tracer()

    async def fake_handler(query: str):
        return json.dumps({"result": [], "metadata": {"provider": "postgres"}})

    with patch("opentelemetry.trace.get_tracer", return_value=mock_tracer):
        traced = trace_tool("list_tables")(fake_handler)
        await traced(query="test")

    attr_keys = [call[0][0] for call in mock_span.set_attribute.call_args_list]
    assert "mcp.tenant_id" not in attr_keys, "tenant_id must not be set when absent"
    assert "mcp.tool.name" in attr_keys, "tool.name must always be set"


@pytest.mark.asyncio
async def test_tool_error_records_error_category():
    """Verify that tool exceptions record error category on span."""
    mock_tracer, mock_span = _make_mock_tracer()

    async def failing_handler():
        raise ValueError("test error")

    with patch("opentelemetry.trace.get_tracer", return_value=mock_tracer):
        traced = trace_tool("broken_tool")(failing_handler)
        with pytest.raises(ValueError):
            await traced()

    mock_span.record_exception.assert_called_once()
    # Verify error category attribute was set
    attr_keys = [call[0][0] for call in mock_span.set_attribute.call_args_list]
    assert "mcp.tool.error.category" in attr_keys, "error category must be recorded"


@pytest.mark.asyncio
async def test_truncation_detected_for_large_payload():
    """Verify that truncation is detected and recorded for bounded output."""
    mock_tracer, mock_span = _make_mock_tracer()

    # Create a response that will be bounded
    large_result = [{"data": "x" * 100} for _ in range(10)]
    envelope = {
        "result": large_result,
        "metadata": {"provider": "postgres"},
    }

    async def large_handler():
        return json.dumps(envelope)

    with patch("opentelemetry.trace.get_tracer", return_value=mock_tracer):
        traced = trace_tool("large_tool")(large_handler)
        await traced()

    attrs = {}
    for call in mock_span.set_attribute.call_args_list:
        key, value = call[0]
        attrs[key] = value

    assert "mcp.tool.response.truncated" in attrs, "truncation flag must always be present"
    assert isinstance(attrs["mcp.tool.response.truncated"], bool), "truncated must be boolean"


@pytest.mark.asyncio
async def test_execute_sql_truncation_detected_from_dict_payload():
    """execute_sql_query truncation should be detected from structured dict payloads."""
    mock_tracer, mock_span = _make_mock_tracer()

    async def execute_handler():
        return {"rows": [], "metadata": {"is_truncated": True}}

    with patch("opentelemetry.trace.get_tracer", return_value=mock_tracer):
        traced = trace_tool("execute_sql_query")(execute_handler)
        await traced()

    attrs = {}
    for call in mock_span.set_attribute.call_args_list:
        key, value = call[0]
        attrs[key] = value

    assert attrs["mcp.tool.response.truncated"] is True
    assert attrs["mcp.tool.response.truncation_parse_failed"] is False


@pytest.mark.asyncio
async def test_execute_sql_truncation_detected_from_json_string():
    """execute_sql_query truncation should be detected from JSON string payloads."""
    mock_tracer, mock_span = _make_mock_tracer()

    async def execute_handler():
        return json.dumps({"rows": [], "metadata": {"is_truncated": True}})

    with patch("opentelemetry.trace.get_tracer", return_value=mock_tracer):
        traced = trace_tool("execute_sql_query")(execute_handler)
        await traced()

    attrs = {}
    for call in mock_span.set_attribute.call_args_list:
        key, value = call[0]
        attrs[key] = value

    assert attrs["mcp.tool.response.truncated"] is True
    assert attrs["mcp.tool.response.truncation_parse_failed"] is False


@pytest.mark.asyncio
async def test_execute_sql_truncation_parse_failure_is_non_fatal():
    """Malformed execute response should not crash and must flag parse failure."""
    mock_tracer, mock_span = _make_mock_tracer()

    async def execute_handler():
        return '{"metadata":{"is_truncated":tru'

    with patch("opentelemetry.trace.get_tracer", return_value=mock_tracer):
        traced = trace_tool("execute_sql_query")(execute_handler)
        await traced()

    attrs = {}
    for call in mock_span.set_attribute.call_args_list:
        key, value = call[0]
        attrs[key] = value

    assert attrs["mcp.tool.response.truncated"] is False
    assert attrs["mcp.tool.response.truncation_parse_failed"] is True
