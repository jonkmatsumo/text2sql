"""Tests for agent-side MCP trace context injection."""

from unittest.mock import AsyncMock, patch

import pytest

from agent.mcp_client.tool_wrapper import create_tool_wrapper


@pytest.mark.asyncio
async def test_tool_wrapper_injects_trace_context_into_mcp_payload():
    """Wrapper should attach reserved _trace_context when a carrier is available."""
    invoke_fn = AsyncMock(return_value={"ok": True})
    tool = create_tool_wrapper(
        name="execute_sql_query",
        description="Execute SQL",
        input_schema={},
        invoke_fn=invoke_fn,
    )

    payload = {"tenant_id": 7}

    def _inject(carrier):
        carrier["traceparent"] = "00-80e1afed08e019fc1110464cfa66635c-7a085853722dc6d2-01"
        carrier["tracestate"] = "vendor=value"

    with patch("agent.telemetry.telemetry.inject_context", side_effect=_inject):
        await tool.ainvoke(payload)

    invoke_fn.assert_called_once()
    forwarded = invoke_fn.call_args.args[0]
    assert forwarded["tenant_id"] == 7
    assert forwarded["_trace_context"]["traceparent"].startswith("00-")
    assert forwarded["_trace_context"]["tracestate"] == "vendor=value"
    assert "_trace_context" not in payload


@pytest.mark.asyncio
async def test_tool_wrapper_omits_trace_context_without_active_carrier():
    """Wrapper should avoid adding reserved trace context when injector returns empty carrier."""
    invoke_fn = AsyncMock(return_value={"ok": True})
    tool = create_tool_wrapper(
        name="execute_sql_query",
        description="Execute SQL",
        input_schema={},
        invoke_fn=invoke_fn,
    )

    payload = {"tenant_id": 7}

    with patch("agent.telemetry.telemetry.inject_context", return_value=None):
        await tool.ainvoke(payload)

    invoke_fn.assert_called_once()
    forwarded = invoke_fn.call_args.args[0]
    assert forwarded == payload
    assert "_trace_context" not in forwarded
