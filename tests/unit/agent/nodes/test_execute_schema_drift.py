"""Tests for schema drift detection in the execute node."""

from unittest.mock import AsyncMock, MagicMock

import pytest

from agent.nodes.execute import validate_and_execute_node
from agent.state import AgentState

PROVIDER_POSTGRES = "postgres"
PROVIDER_BIGQUERY = "bigquery"


@pytest.fixture
def mock_executor_tool():
    """Mock the execute_sql_query tool."""
    tool = AsyncMock()
    tool.name = "execute_sql_query"
    return tool


@pytest.fixture
def mock_tools(mock_executor_tool):
    """Return a list of mock tools."""
    return [mock_executor_tool]


@pytest.fixture
def base_state():
    """Return a valid base AgentState."""
    return AgentState(
        current_sql="SELECT * FROM users",
        tenant_id=1,
        schema_snapshot_id="snap-123",
        messages=[],
    )


@pytest.mark.asyncio
async def test_drift_detected_postgres(base_state, mock_executor_tool, monkeypatch):
    """Test that schema drift is detected for Postgres errors."""
    mock_get_tools = AsyncMock(return_value=[mock_executor_tool])
    monkeypatch.setattr("agent.nodes.execute.get_mcp_tools", mock_get_tools)

    mock_get_env_str = MagicMock(
        side_effect=lambda k, d=None: "postgres" if k == "QUERY_TARGET_BACKEND" else d
    )
    monkeypatch.setattr("agent.nodes.execute.get_env_str", mock_get_env_str)

    monkeypatch.setattr(
        "agent.nodes.execute.get_env_bool", MagicMock(return_value=True)
    )  # AGENT_SCHEMA_DRIFT_HINTS
    monkeypatch.setattr("agent.validation.policy_enforcer.PolicyEnforcer.validate_sql", MagicMock())

    # Rewrite SQL is async
    mock_rewrite = AsyncMock(return_value="SELECT * FROM users")
    monkeypatch.setattr("agent.validation.tenant_rewriter.TenantRewriter.rewrite_sql", mock_rewrite)

    # Simulate Postgres error
    mock_executor_tool.ainvoke.return_value = {
        "error": 'relation "users" does not exist',
        "error_category": "invalid_request",
        "provider": "postgres",
    }

    result = await validate_and_execute_node(base_state)

    assert result["error"] == 'relation "users" does not exist'
    assert result["schema_drift_suspected"] is True
    assert "users" in result["missing_identifiers"]


@pytest.mark.asyncio
async def test_drift_detected_bigquery(base_state, mock_executor_tool, monkeypatch):
    """Test that schema drift is detected for BigQuery errors."""
    mock_get_tools = AsyncMock(return_value=[mock_executor_tool])
    monkeypatch.setattr("agent.nodes.execute.get_mcp_tools", mock_get_tools)

    mock_get_env_str = MagicMock(
        side_effect=lambda k, d=None: "bigquery" if k == "QUERY_TARGET_BACKEND" else d
    )
    monkeypatch.setattr("agent.nodes.execute.get_env_str", mock_get_env_str)

    monkeypatch.setattr("agent.nodes.execute.get_env_bool", MagicMock(return_value=True))
    monkeypatch.setattr("agent.validation.policy_enforcer.PolicyEnforcer.validate_sql", MagicMock())

    mock_rewrite = AsyncMock(return_value="SELECT * FROM dataset.table")
    monkeypatch.setattr("agent.validation.tenant_rewriter.TenantRewriter.rewrite_sql", mock_rewrite)

    # Simulate BigQuery error
    mock_executor_tool.ainvoke.return_value = {
        "error": "Not found: Table my-project:dataset.table",
        "error_category": "invalid_request",
    }

    result = await validate_and_execute_node(base_state)

    assert result["schema_drift_suspected"] is True
    assert "my-project:dataset.table" in result["missing_identifiers"]


@pytest.mark.asyncio
async def test_no_drift_permission_error(base_state, mock_executor_tool, monkeypatch):
    """Test that permission errors do not trigger schema drift."""
    mock_get_tools = AsyncMock(return_value=[mock_executor_tool])
    monkeypatch.setattr("agent.nodes.execute.get_mcp_tools", mock_get_tools)

    monkeypatch.setattr("agent.nodes.execute.get_env_str", MagicMock(return_value="postgres"))
    monkeypatch.setattr("agent.validation.policy_enforcer.PolicyEnforcer.validate_sql", MagicMock())

    mock_rewrite = AsyncMock(return_value="SELECT * FROM secret")
    monkeypatch.setattr("agent.validation.tenant_rewriter.TenantRewriter.rewrite_sql", mock_rewrite)

    mock_executor_tool.ainvoke.return_value = {
        "error": "permission denied for relation secret",
        "error_category": "permission_denied",
    }

    result = await validate_and_execute_node(base_state)

    assert result.get("schema_drift_suspected") is None  # Should not be set
    assert result.get("missing_identifiers") is None


@pytest.mark.asyncio
async def test_drift_detection_disabled(base_state, mock_executor_tool, monkeypatch):
    """Test that drift detection is skipped if disabled via env var."""
    mock_get_tools = AsyncMock(return_value=[mock_executor_tool])
    monkeypatch.setattr("agent.nodes.execute.get_mcp_tools", mock_get_tools)

    monkeypatch.setattr(
        "agent.nodes.execute.get_env_bool", MagicMock(return_value=False)
    )  # AGENT_SCHEMA_DRIFT_HINTS=False
    monkeypatch.setattr("agent.validation.policy_enforcer.PolicyEnforcer.validate_sql", MagicMock())

    mock_rewrite = AsyncMock(return_value="SELECT * FROM users")
    monkeypatch.setattr("agent.validation.tenant_rewriter.TenantRewriter.rewrite_sql", mock_rewrite)

    mock_executor_tool.ainvoke.return_value = {
        "error": 'relation "users" does not exist',
    }

    result = await validate_and_execute_node(base_state)

    assert result.get("schema_drift_suspected") is None
