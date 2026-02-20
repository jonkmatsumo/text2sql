"""Tests for agent-side MCP tool input schema validation."""

from unittest.mock import AsyncMock

import pytest

from agent.mcp_client.tool_wrapper import create_tool_wrapper
from agent.models.run_budget import RunBudget, run_budget_context


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

    invoke_fn.assert_called_once()
    forwarded = invoke_fn.call_args.args[0]
    assert forwarded["tenant_id"] == payload["tenant_id"]
    assert isinstance(forwarded.get("_request_id"), str)
    assert result == {"ok": True}


@pytest.mark.asyncio
async def test_tool_call_budget_exceeded_returns_typed_error():
    """Tool wrapper should short-circuit with budget_exceeded when call budget is exhausted."""
    invoke_fn = AsyncMock(return_value={"ok": True})
    tool = create_tool_wrapper(
        name="execute_sql_query",
        description="Execute SQL",
        input_schema={},
        invoke_fn=invoke_fn,
    )
    budget = RunBudget(
        llm_token_budget=100,
        tool_call_budget=0,
        sql_row_budget=100,
        time_budget_ms=30_000,
    )

    with run_budget_context(budget):
        result = await tool.ainvoke({"tenant_id": 1})

    invoke_fn.assert_not_called()
    assert result["error"]["category"] == "budget_exceeded"
    assert result["error"]["code"] == "BUDGET_EXCEEDED"


@pytest.mark.asyncio
async def test_tool_call_budget_consumption_persists_across_attempts():
    """Budget accounting should persist across repeated tool calls in one run context."""
    invoke_fn = AsyncMock(return_value={"ok": True})
    tool = create_tool_wrapper(
        name="execute_sql_query",
        description="Execute SQL",
        input_schema={},
        invoke_fn=invoke_fn,
    )
    budget = RunBudget(
        llm_token_budget=100,
        tool_call_budget=1,
        sql_row_budget=100,
        time_budget_ms=30_000,
    )

    with run_budget_context(budget):
        first = await tool.ainvoke({"tenant_id": 1})
        second = await tool.ainvoke({"tenant_id": 1})

    assert first == {"ok": True}
    assert second["error"]["category"] == "budget_exceeded"
    assert invoke_fn.call_count == 1


@pytest.mark.asyncio
async def test_rows_returned_budget_exceeded_from_tool_metadata():
    """Wrapper should enforce rows-returned budget using tool metadata counters."""
    invoke_fn = AsyncMock(return_value={"metadata": {"items_returned": 3}, "result": []})
    tool = create_tool_wrapper(
        name="list_tables",
        description="List tables",
        input_schema={},
        invoke_fn=invoke_fn,
    )
    budget = RunBudget(
        llm_token_budget=100,
        tool_call_budget=10,
        sql_row_budget=1,
        time_budget_ms=30_000,
    )

    with run_budget_context(budget):
        result = await tool.ainvoke({"tenant_id": 1})

    assert result["error"]["category"] == "budget_exceeded"
    assert result["error"]["details_safe"]["budget_dimension"] == "rows_returned"
    assert invoke_fn.call_count == 1
