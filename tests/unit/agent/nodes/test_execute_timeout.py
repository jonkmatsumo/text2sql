"""Tests for execution node timeout propagation."""

import time
from unittest.mock import AsyncMock, MagicMock

import pytest

from agent.nodes.execute import validate_and_execute_node
from agent.state import AgentState


@pytest.fixture
def mock_telemetry(monkeypatch):
    """Fixture to mock telemetry module."""
    mock = MagicMock()
    monkeypatch.setattr("agent.nodes.execute.telemetry", mock)
    return mock


@pytest.fixture
def mock_policy_enforcer(monkeypatch):
    """Fixture to mock PolicyEnforcer class."""
    mock = MagicMock()
    monkeypatch.setattr("agent.nodes.execute.PolicyEnforcer", mock)
    return mock


@pytest.fixture
def mock_tenant_rewriter(monkeypatch):
    """Fixture to mock TenantRewriter class."""
    mock = MagicMock()
    monkeypatch.setattr("agent.nodes.execute.TenantRewriter", mock)

    # TenantRewriter.rewrite_sql is called as a class method/static method on the class itself
    # So we attach the AsyncMock directly to the mock object (which replaces the class)
    mock.rewrite_sql = AsyncMock()

    async def echo(sql, tenant):
        return sql

    mock.rewrite_sql.side_effect = echo

    return mock


@pytest.fixture
def mock_mcp_tools(monkeypatch):
    """Fixture to mock get_mcp_tools function."""
    # get_mcp_tools is awaited, so it should return a coroutine or be an AsyncMock
    mock = AsyncMock()
    monkeypatch.setattr("agent.nodes.execute.get_mcp_tools", mock)
    return mock


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
