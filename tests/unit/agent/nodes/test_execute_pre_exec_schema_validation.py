"""Tests for pre-execution schema validation behavior in execute node."""

import json
from unittest.mock import AsyncMock, patch

import pytest

from agent.nodes.execute import validate_and_execute_node
from agent.state import AgentState


def _envelope(rows=None):
    return json.dumps(
        {
            "schema_version": "1.0",
            "rows": rows if rows is not None else [],
            "metadata": {"rows_returned": len(rows) if rows else 0, "is_truncated": False},
        }
    )


def _env_bool(flag_name: str, default: bool = False, *, block_on_mismatch: bool) -> bool:
    if flag_name == "AGENT_BLOCK_ON_SCHEMA_MISMATCH":
        return block_on_mismatch
    return default


@pytest.mark.asyncio
@patch("agent.nodes.execute.get_mcp_tools")
@patch("agent.nodes.execute.PolicyEnforcer")
@patch("agent.nodes.execute.TenantRewriter")
@patch("agent.utils.schema_fingerprint.validate_sql_against_schema")
async def test_schema_mismatch_warn_only_when_block_flag_off(
    mock_validate_schema,
    mock_rewriter,
    mock_enforcer,
    mock_get_tools,
):
    """With blocking flag off, pre-exec mismatch should log warning and continue execution."""
    mock_enforcer.validate_sql.return_value = None
    mock_rewriter.rewrite_sql = AsyncMock(side_effect=lambda sql, _tenant_id: sql)
    mock_validate_schema.return_value = (
        False,
        frozenset({"missing_table"}),
        "Pre-execution validation warning.",
    )

    mock_tool = AsyncMock()
    mock_tool.name = "execute_sql_query"
    mock_tool.ainvoke = AsyncMock(return_value=_envelope(rows=[{"ok": 1}]))
    mock_get_tools.return_value = [mock_tool]

    state = AgentState(
        messages=[],
        schema_context="",
        raw_schema_context=[{"type": "Table", "name": "users"}],
        current_sql="SELECT * FROM users",
        query_result=None,
        error=None,
        retry_count=0,
        tenant_id=1,
    )

    with patch(
        "agent.nodes.execute.get_env_bool",
        side_effect=lambda name, default=False: _env_bool(name, default, block_on_mismatch=False),
    ):
        result = await validate_and_execute_node(state)

    assert result["error"] is None
    assert result["query_result"] == [{"ok": 1}]
    mock_tool.ainvoke.assert_called_once()


@pytest.mark.asyncio
@patch("agent.nodes.execute.get_mcp_tools")
@patch("agent.nodes.execute.PolicyEnforcer")
@patch("agent.nodes.execute.TenantRewriter")
@patch("agent.utils.schema_fingerprint.validate_sql_against_schema")
async def test_schema_mismatch_blocks_execution_when_flag_on(
    mock_validate_schema,
    mock_rewriter,
    mock_enforcer,
    mock_get_tools,
):
    """With blocking flag on, pre-exec mismatch should fail before execute_sql_query call."""
    mock_enforcer.validate_sql.return_value = None
    mock_rewriter.rewrite_sql = AsyncMock(side_effect=lambda sql, _tenant_id: sql)
    mock_validate_schema.return_value = (
        False,
        frozenset({"missing_table"}),
        "Pre-execution validation warning.",
    )

    mock_tool = AsyncMock()
    mock_tool.name = "execute_sql_query"
    mock_tool.ainvoke = AsyncMock(return_value=_envelope(rows=[{"ok": 1}]))
    mock_get_tools.return_value = [mock_tool]

    state = AgentState(
        messages=[],
        schema_context="",
        raw_schema_context=[{"type": "Table", "name": "users"}],
        current_sql="SELECT * FROM users",
        query_result=None,
        error=None,
        retry_count=0,
        tenant_id=1,
    )

    with patch(
        "agent.nodes.execute.get_env_bool",
        side_effect=lambda name, default=False: _env_bool(name, default, block_on_mismatch=True),
    ):
        result = await validate_and_execute_node(state)

    assert result["error_category"] == "schema_drift"
    assert result["query_result"] is None
    assert result["missing_identifiers"] == ["missing_table"]
    mock_tool.ainvoke.assert_not_called()


@pytest.mark.asyncio
@patch("agent.nodes.execute.get_mcp_tools")
@patch("agent.nodes.execute.PolicyEnforcer")
@patch("agent.nodes.execute.TenantRewriter")
async def test_schema_mismatch_blocks_for_complex_cte_union_query(
    mock_rewriter,
    mock_enforcer,
    mock_get_tools,
):
    """Blocking mode should reject complex CTE/UNION mismatches before execution."""
    mock_enforcer.validate_sql.return_value = None
    complex_sql = """
        WITH active_users AS (
            SELECT id FROM users
        )
        SELECT id FROM active_users
        UNION
        SELECT id FROM payments
    """
    mock_rewriter.rewrite_sql = AsyncMock(side_effect=lambda sql, _tenant_id: sql)

    mock_tool = AsyncMock()
    mock_tool.name = "execute_sql_query"
    mock_tool.ainvoke = AsyncMock(return_value=_envelope(rows=[{"ok": 1}]))
    mock_get_tools.return_value = [mock_tool]

    state = AgentState(
        messages=[],
        schema_context="",
        raw_schema_context=[
            {"type": "Table", "name": "users"},
            {"type": "Column", "table": "users", "name": "id"},
        ],
        current_sql=complex_sql,
        query_result=None,
        error=None,
        retry_count=0,
        tenant_id=1,
    )

    with patch(
        "agent.nodes.execute.get_env_bool",
        side_effect=lambda name, default=False: _env_bool(name, default, block_on_mismatch=True),
    ):
        result = await validate_and_execute_node(state)

    assert result["error_category"] == "schema_drift"
    assert result["query_result"] is None
    assert "payments" in result["missing_identifiers"]
    mock_tool.ainvoke.assert_not_called()
