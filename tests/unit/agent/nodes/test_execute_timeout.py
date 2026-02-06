"""Tests for execution node timeout propagation."""

import time
from unittest.mock import MagicMock

import pytest

from agent.nodes.execute import validate_and_execute_node
from agent.state import AgentState


@pytest.fixture
def mock_telemetry(mocker):
    """Fixture to mock telemetry module."""
    return mocker.patch("agent.nodes.execute.telemetry")


@pytest.fixture
def mock_policy_enforcer(mocker):
    """Fixture to mock PolicyEnforcer class."""
    return mocker.patch("agent.nodes.execute.PolicyEnforcer")


@pytest.fixture
def mock_tenant_rewriter(mocker):
    """Fixture to mock TenantRewriter class."""
    rewriter = mocker.patch("agent.nodes.execute.TenantRewriter")

    async def echo(sql, tenant):
        return sql

    rewriter.rewrite_sql.side_effect = echo
    return rewriter


@pytest.fixture
def mock_mcp_tools(mocker):
    """Fixture to mock get_mcp_tools function."""
    return mocker.patch("agent.nodes.execute.get_mcp_tools")


@pytest.mark.asyncio
async def test_execute_propagates_timeout(
    mock_telemetry, mock_policy_enforcer, mock_tenant_rewriter, mock_mcp_tools
):
    """Test that deadline_ts is converted to timeout_seconds."""
    # Mock tool
    mock_tool = MagicMock()
    mock_tool.name = "execute_sql_query"
    mock_tool.ainvoke = MagicMock()

    # Async mock for ainvoke
    async def side_effect(arg):
        return {"rows": [], "metadata": {}}

    mock_tool.ainvoke.side_effect = side_effect

    mock_mcp_tools.return_value = [mock_tool]

    # Set deadline 5 seconds in future
    deadline = time.monotonic() + 5.0
    state = AgentState(
        messages=[], current_sql="SELECT 1", tenant_id="tenant-1", deadline_ts=deadline
    )

    await validate_and_execute_node(state)

    # Check call args
    args = mock_tool.ainvoke.call_args[0][0]
    timeout = args.get("timeout_seconds")
    assert timeout is not None
    # Should be approx 5.0 (allow some delta)
    assert 4.0 < timeout < 6.0


@pytest.mark.asyncio
async def test_execute_early_timeout(
    mock_telemetry, mock_policy_enforcer, mock_tenant_rewriter, mock_mcp_tools
):
    """Test that execution aborts if deadline is too close."""
    mock_tool = MagicMock()
    mock_tool.name = "execute_sql_query"
    mock_mcp_tools.return_value = [mock_tool]

    # Set deadline in past
    deadline = time.monotonic() - 1.0
    state = AgentState(
        messages=[], current_sql="SELECT 1", tenant_id="tenant-1", deadline_ts=deadline
    )

    result = await validate_and_execute_node(state)

    assert result["error_category"] == "timeout"
    assert "Execution timed out" in result["error"]
    # Ensure tool was NOT called
    assert not mock_tool.ainvoke.called


@pytest.mark.asyncio
async def test_execute_no_deadline(
    mock_telemetry, mock_policy_enforcer, mock_tenant_rewriter, mock_mcp_tools
):
    """Test behavior when no deadline is set."""
    mock_tool = MagicMock()
    mock_tool.name = "execute_sql_query"

    async def side_effect(arg):
        return {"rows": [], "metadata": {}}

    mock_tool.ainvoke.side_effect = side_effect
    mock_mcp_tools.return_value = [mock_tool]

    state = AgentState(
        messages=[],
        current_sql="SELECT 1",
        tenant_id="tenant-1",
        # No deadline_ts
    )

    await validate_and_execute_node(state)

    args = mock_tool.ainvoke.call_args[0][0]
    # timeout_seconds should be None
    assert args.get("timeout_seconds") is None
