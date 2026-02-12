"""Tests for agent-side MCP tool input schema validation."""

from unittest.mock import AsyncMock

import pytest

from agent.mcp_client.tool_wrapper import create_tool_wrapper


@pytest.mark.asyncio
async def test_invalid_tool_input_rejected_locally_without_mcp_call():
    """Malformed args should fail fast in the wrapper without invoking MCP."""
    invoke_fn = AsyncMock(return_value={"ok": True})
    tool = create_tool_wrapper(
        name="execute_sql_query",
        description="Execute SQL",
        input_schema={
            "type": "object",
            "properties": {"tenant_id": {"type": "integer"}},
            "required": ["tenant_id"],
            "additionalProperties": False,
        },
        invoke_fn=invoke_fn,
    )

    result = await tool.ainvoke({"tenant_id": "not-an-int"})

    invoke_fn.assert_not_called()
    assert isinstance(result, dict)
    assert result["error"]["category"] == "invalid_request"
    assert result["error"]["code"] == "TOOL_INPUT_SCHEMA_VIOLATION"
    assert result["error"]["reason_code"] == "tool_input_schema_violation"


@pytest.mark.asyncio
async def test_valid_tool_input_passes_through_to_mcp():
    """Schema-valid args should pass through to the underlying MCP invoke function."""
    invoke_fn = AsyncMock(return_value={"ok": True})
    tool = create_tool_wrapper(
        name="execute_sql_query",
        description="Execute SQL",
        input_schema={
            "type": "object",
            "properties": {"tenant_id": {"type": "integer"}},
            "required": ["tenant_id"],
        },
        invoke_fn=invoke_fn,
    )

    payload = {"tenant_id": 7}
    result = await tool.ainvoke(payload)

    invoke_fn.assert_called_once_with(payload)
    assert result == {"ok": True}
