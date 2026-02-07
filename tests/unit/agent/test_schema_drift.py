"""Tests for schema drift detection telemetry."""

import json
from unittest.mock import AsyncMock, patch

import pytest

from agent.nodes.execute import validate_and_execute_node
from agent.state import AgentState


def _envelope(rows=None, metadata=None, error=None):
    data = {
        "schema_version": "1.0",
        "rows": rows if rows is not None else [],
        "metadata": (
            metadata
            if metadata
            else {"rows_returned": len(rows) if rows else 0, "is_truncated": False}
        ),
    }
    if error:
        data["error"] = error
        if "rows_returned" not in data["metadata"]:
            data["metadata"]["rows_returned"] = 0
    return json.dumps(data)


@pytest.mark.asyncio
@patch("agent.nodes.execute.get_mcp_tools")
@patch("agent.nodes.execute.PolicyEnforcer")
@patch("agent.nodes.execute.TenantRewriter")
async def test_schema_drift_hint_on_missing_table(
    mock_rewriter, mock_enforcer, mock_get_tools, schema_fixture, monkeypatch
):
    """Missing table errors should set schema drift hints."""
    monkeypatch.setenv("AGENT_SCHEMA_DRIFT_HINTS", "true")
    mock_enforcer.validate_sql.return_value = None
    mock_rewriter.rewrite_sql = AsyncMock(side_effect=lambda sql, tid: sql)

    mock_tool = AsyncMock()
    mock_tool.name = "execute_sql_query"

    error = {
        "message": 'Database Error: relation "orders" does not exist',
        "category": "unknown",
        "provider": "postgres",
        "is_retryable": False,
    }
    mock_tool.ainvoke = AsyncMock(return_value=_envelope(error=error))
    mock_get_tools.return_value = [mock_tool]

    state = AgentState(
        messages=[],
        schema_context="",
        current_sql=schema_fixture.sample_query,
        query_result=None,
        error=None,
        retry_count=0,
        schema_snapshot_id="fp-1234",
    )

    result = await validate_and_execute_node(state)

    assert result["schema_drift_suspected"] is True
    assert "orders" in result["missing_identifiers"]


@pytest.mark.asyncio
@patch("agent.nodes.execute.get_mcp_tools")
@patch("agent.nodes.execute.PolicyEnforcer")
@patch("agent.nodes.execute.TenantRewriter")
async def test_schema_drift_hint_on_bigquery_missing_column(
    mock_rewriter, mock_enforcer, mock_get_tools, schema_fixture, monkeypatch
):
    """Ensure BigQuery missing column errors set schema drift hints."""
    monkeypatch.setenv("AGENT_SCHEMA_DRIFT_HINTS", "true")
    monkeypatch.setenv("QUERY_TARGET_BACKEND", "bigquery")
    mock_enforcer.validate_sql.return_value = None
    mock_rewriter.rewrite_sql = AsyncMock(side_effect=lambda sql, tid: sql)

    mock_tool = AsyncMock()
    mock_tool.name = "execute_sql_query"
    error = {
        "message": "Unrecognized name: user_id",
        "category": "unknown",
        "provider": "bigquery",
        "is_retryable": False,
    }
    mock_tool.ainvoke = AsyncMock(return_value=_envelope(error=error))
    mock_get_tools.return_value = [mock_tool]

    state = AgentState(
        messages=[],
        schema_context="",
        current_sql=schema_fixture.sample_query,
        query_result=None,
        error=None,
        retry_count=0,
    )

    result = await validate_and_execute_node(state)

    assert result["schema_drift_suspected"] is True
    assert "user_id" in result["missing_identifiers"]


@pytest.mark.asyncio
@patch("agent.nodes.execute.get_mcp_tools")
@patch("agent.nodes.execute.PolicyEnforcer")
@patch("agent.nodes.execute.TenantRewriter")
async def test_schema_drift_hint_on_snowflake_missing_table(
    mock_rewriter, mock_enforcer, mock_get_tools, schema_fixture, monkeypatch
):
    """Snowflake missing table errors should set schema drift hints."""
    monkeypatch.setenv("AGENT_SCHEMA_DRIFT_HINTS", "true")
    monkeypatch.setenv("QUERY_TARGET_BACKEND", "snowflake")
    mock_enforcer.validate_sql.return_value = None
    mock_rewriter.rewrite_sql = AsyncMock(side_effect=lambda sql, tid: sql)

    mock_tool = AsyncMock()
    mock_tool.name = "execute_sql_query"
    error = {
        "message": "SQL compilation error: Object 'DB.SCHEMA.TABLE' does not exist",
        "category": "unknown",
        "provider": "snowflake",
        "is_retryable": False,
    }
    mock_tool.ainvoke = AsyncMock(return_value=_envelope(error=error))
    mock_get_tools.return_value = [mock_tool]

    state = AgentState(
        messages=[],
        schema_context="",
        current_sql=schema_fixture.sample_query,
        query_result=None,
        error=None,
        retry_count=0,
    )

    result = await validate_and_execute_node(state)

    assert result["schema_drift_suspected"] is True
    # Identifier extraction logic depends on regex
    assert len(result["missing_identifiers"]) > 0


@pytest.mark.asyncio
@patch("agent.nodes.execute.get_mcp_tools")
@patch("agent.nodes.execute.PolicyEnforcer")
@patch("agent.nodes.execute.TenantRewriter")
async def test_schema_drift_hint_suppressed_by_env(
    mock_rewriter, mock_enforcer, mock_get_tools, schema_fixture, monkeypatch
):
    """Hint generation suppressed when AGENT_SCHEMA_DRIFT_HINTS=false."""
    monkeypatch.setenv("AGENT_SCHEMA_DRIFT_HINTS", "false")
    mock_enforcer.validate_sql.return_value = None
    mock_rewriter.rewrite_sql = AsyncMock(side_effect=lambda sql, tid: sql)

    mock_tool = AsyncMock()
    mock_tool.name = "execute_sql_query"
    error = {
        "message": 'relation "orders" does not exist',
        "category": "unknown",
        "provider": "postgres",
        "is_retryable": False,
    }
    mock_tool.ainvoke = AsyncMock(return_value=_envelope(error=error))
    mock_get_tools.return_value = [mock_tool]

    state = AgentState(
        messages=[],
        schema_context="",
        current_sql=schema_fixture.sample_query,
        query_result=None,
        error=None,
        retry_count=0,
    )

    result = await validate_and_execute_node(state)

    assert "schema_drift_suspected" not in result


@pytest.mark.asyncio
@patch("agent.nodes.execute.get_mcp_tools")
@patch("agent.nodes.execute.PolicyEnforcer")
@patch("agent.nodes.execute.TenantRewriter")
async def test_schema_drift_auto_refresh_enabled(
    mock_rewriter, mock_enforcer, mock_get_tools, schema_fixture, monkeypatch
):
    """Auto-refresh flag propagated when enabled."""
    monkeypatch.setenv("AGENT_SCHEMA_DRIFT_HINTS", "true")
    monkeypatch.setenv("AGENT_SCHEMA_DRIFT_AUTO_REFRESH", "true")
    mock_enforcer.validate_sql.return_value = None
    mock_rewriter.rewrite_sql = AsyncMock(side_effect=lambda sql, tid: sql)

    mock_tool = AsyncMock()
    mock_tool.name = "execute_sql_query"
    error = {
        "message": 'relation "orders" does not exist',
        "category": "unknown",
        "provider": "postgres",
        "is_retryable": False,
    }
    mock_tool.ainvoke = AsyncMock(return_value=_envelope(error=error))
    mock_get_tools.return_value = [mock_tool]

    state = AgentState(
        messages=[],
        schema_context="",
        current_sql=schema_fixture.sample_query,
        query_result=None,
        error=None,
        retry_count=0,
    )

    result = await validate_and_execute_node(state)

    assert result["schema_drift_suspected"] is True
    assert result["schema_drift_auto_refresh"] is True
