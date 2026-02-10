"""Regression coverage for operability and observability hardening."""

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from agent.graph import route_after_execution
from agent.telemetry_schema import AGENT_GRAPH_NODE_SPAN_NAMES, SPAN_CONTRACTS
from mcp_server.tools.list_tables import handler as list_tables_handler
from mcp_server.utils.tracing import trace_tool


def _make_mock_tracer():
    """Create a mock tracer/span pair that records set_attribute calls."""
    mock_tracer = MagicMock()
    mock_span = MagicMock()
    mock_span.is_recording.return_value = True
    mock_tracer.start_as_current_span.return_value.__enter__ = MagicMock(return_value=mock_span)
    mock_tracer.start_as_current_span.return_value.__exit__ = MagicMock(return_value=False)
    return mock_tracer, mock_span


def _attrs_from_span(mock_span):
    attrs = {}
    for call in mock_span.set_attribute.call_args_list:
        key, value = call[0]
        attrs[key] = value
    return attrs


def test_retry_policy_default_is_adaptive(monkeypatch):
    """Default retry behavior should be adaptive and honor retryability flags."""
    monkeypatch.delenv("AGENT_RETRY_POLICY", raising=False)

    state = {
        "error": "permanent failure",
        "error_category": "unknown",
        "error_metadata": {"is_retryable": False},
        "retry_count": 0,
    }

    route = route_after_execution(state)

    assert route == "failed"
    assert state["retry_summary"]["policy"] == "adaptive"
    assert state["retry_summary"]["is_retryable"] is False


def test_span_contracts_cover_all_nodes():
    """Every graph node should resolve to a registered span contract."""
    missing = [
        node_name
        for node_name, span_name in AGENT_GRAPH_NODE_SPAN_NAMES.items()
        if span_name not in SPAN_CONTRACTS
    ]
    assert missing == []


@pytest.mark.asyncio
async def test_truncation_detection_parses_json():
    """Tracing should parse truncation fields from JSON and flag parse failures safely."""
    mock_tracer, mock_span = _make_mock_tracer()

    async def ok_handler():
        return json.dumps({"rows": [], "metadata": {"is_truncated": True}})

    with patch("opentelemetry.trace.get_tracer", return_value=mock_tracer):
        traced = trace_tool("execute_sql_query")(ok_handler)
        await traced()

    attrs = _attrs_from_span(mock_span)
    assert attrs["mcp.tool.response.truncated"] is True
    assert attrs["mcp.tool.response.truncation_parse_failed"] is False

    mock_span.set_attribute.reset_mock()

    async def malformed_handler():
        return '{"metadata":{"is_truncated":tru'

    with patch("opentelemetry.trace.get_tracer", return_value=mock_tracer):
        traced = trace_tool("execute_sql_query")(malformed_handler)
        await traced()

    malformed_attrs = _attrs_from_span(mock_span)
    assert malformed_attrs["mcp.tool.response.truncated"] is False
    assert malformed_attrs["mcp.tool.response.truncation_parse_failed"] is True


@pytest.mark.asyncio
async def test_non_execute_tools_output_bounded():
    """Non-execute tools should be bounded and return truncation metadata."""
    large_tables = [f"table_{i}" for i in range(500)]
    mock_store = AsyncMock()
    mock_store.list_tables.return_value = large_tables

    with (
        patch("dal.database.Database.get_metadata_store", return_value=mock_store),
        patch("mcp_server.utils.tracing.get_env_str", return_value="warn"),
        patch("mcp_server.utils.tool_output.get_env_int", return_value=400),
    ):
        traced_handler = trace_tool("list_tables")(list_tables_handler)
        response = json.loads(await traced_handler(tenant_id=1))

    assert response["metadata"]["is_truncated"] is True
    assert response["metadata"]["items_total"] == len(large_tables)
    assert response["metadata"]["items_returned"] < len(large_tables)
    assert response["metadata"]["bytes_total"] >= response["metadata"]["bytes_returned"]
