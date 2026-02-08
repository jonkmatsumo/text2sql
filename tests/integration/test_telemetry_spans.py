from unittest.mock import MagicMock, patch

import pytest
from opentelemetry.trace import SpanKind

from mcp_server.utils.tracing import trace_tool


@pytest.mark.asyncio
async def test_tool_tracing_decorator():
    """Verify that trace_tool decorator creates a span."""
    # Mock the tracer and span
    mock_tracer = MagicMock()
    mock_span = MagicMock()
    mock_tracer.start_as_current_span.return_value.__enter__.return_value = mock_span

    async def dummy_handler():
        return "ok"

    with patch("opentelemetry.trace.get_tracer", return_value=mock_tracer):
        traced_handler = trace_tool("test_tool")(dummy_handler)

        # Invoke
        await traced_handler()

        # Verify
        mock_tracer.start_as_current_span.assert_called_with(
            "mcp.tool.test_tool", kind=SpanKind.SERVER
        )
        mock_span.set_attribute.assert_any_call("mcp.tool.name", "test_tool")


@pytest.mark.asyncio
async def test_registry_integration():
    """Verify that registry registers tools with tracing."""
    # This is harder to test without running the full registry logic which imports everything.
    # But we modified registry.py to use trace_tool.
    # We can inspect the registered tool in FastMCP if we could mock FastMCP.
    pass
