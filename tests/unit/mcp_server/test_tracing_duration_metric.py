"""Tests for MCP tool duration histogram emission."""

import json
from unittest.mock import MagicMock, patch

import pytest

from mcp_server.utils.tracing import trace_tool


def _make_mock_tracer():
    mock_tracer = MagicMock()
    mock_span = MagicMock()
    mock_span.is_recording.return_value = True
    mock_tracer.start_as_current_span.return_value.__enter__ = MagicMock(return_value=mock_span)
    mock_tracer.start_as_current_span.return_value.__exit__ = MagicMock(return_value=False)
    return mock_tracer


@pytest.mark.asyncio
async def test_trace_tool_records_duration_histogram():
    """trace_tool should emit an mcp.tool.duration_ms histogram for each tool call."""
    mock_tracer = _make_mock_tracer()

    async def handler():
        return json.dumps({"result": [{"ok": True}], "metadata": {"provider": "postgres"}})

    with (
        patch("opentelemetry.trace.get_tracer", return_value=mock_tracer),
        patch("mcp_server.utils.tracing.mcp_metrics.add_counter") as mock_add_counter,
        patch("mcp_server.utils.tracing.mcp_metrics.record_histogram") as mock_record_histogram,
    ):
        traced = trace_tool("list_tables")(handler)
        await traced()

    histogram_names = [call.args[0] for call in mock_record_histogram.call_args_list]
    assert "mcp.tool.duration_ms" in histogram_names
    counter_names = [call.args[0] for call in mock_add_counter.call_args_list]
    assert "mcp.tool.calls_total" in counter_names
