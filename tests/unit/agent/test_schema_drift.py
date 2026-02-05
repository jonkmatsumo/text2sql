"""Tests for schema drift hinting."""

import json
from unittest.mock import AsyncMock, patch

import pytest

from agent.nodes.execute import validate_and_execute_node
from agent.state import AgentState


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
    payload = json.dumps([{"error": 'Database Error: relation "orders" does not exist'}])
    mock_tool.ainvoke = AsyncMock(return_value=payload)
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
    assert result["missing_identifiers"] == ["orders"]
    assert result["schema_snapshot_id"] == "fp-1234"


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
    payload = json.dumps([{"error": "Unrecognized name: user_id"}])
    mock_tool.ainvoke = AsyncMock(return_value=payload)
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
    assert result["missing_identifiers"] == ["user_id"]


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
    payload = json.dumps(
        [{"error": "SQL compilation error: Object 'DB.SCHEMA.TABLE' does not exist"}]
    )
    mock_tool.ainvoke = AsyncMock(return_value=payload)
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
    assert result["missing_identifiers"] == ["DB.SCHEMA.TABLE"]


@pytest.mark.asyncio
@patch("agent.nodes.execute.get_mcp_tools")
@patch("agent.nodes.execute.PolicyEnforcer")
@patch("agent.nodes.execute.TenantRewriter")
async def test_schema_drift_hint_skipped_for_non_schema_error(
    mock_rewriter, mock_enforcer, mock_get_tools, schema_fixture, monkeypatch
):
    """Non schema errors should not set drift hints."""
    monkeypatch.setenv("AGENT_SCHEMA_DRIFT_HINTS", "true")
    mock_enforcer.validate_sql.return_value = None
    mock_rewriter.rewrite_sql = AsyncMock(side_effect=lambda sql, tid: sql)

    mock_tool = AsyncMock()
    mock_tool.name = "execute_sql_query"
    payload = json.dumps([{"error": "Database Error: connection refused"}])
    mock_tool.ainvoke = AsyncMock(return_value=payload)
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
    assert "missing_identifiers" not in result


@pytest.mark.asyncio
@patch("agent.nodes.execute.get_mcp_tools")
@patch("agent.nodes.execute.PolicyEnforcer")
@patch("agent.nodes.execute.TenantRewriter")
async def test_schema_drift_hint_disabled(
    mock_rewriter, mock_enforcer, mock_get_tools, schema_fixture, monkeypatch
):
    """Hints should be gated by AGENT_SCHEMA_DRIFT_HINTS."""
    monkeypatch.setenv("AGENT_SCHEMA_DRIFT_HINTS", "false")
    mock_enforcer.validate_sql.return_value = None
    mock_rewriter.rewrite_sql = AsyncMock(side_effect=lambda sql, tid: sql)

    mock_tool = AsyncMock()
    mock_tool.name = "execute_sql_query"
    payload = json.dumps([{"error": 'Database Error: column "foo" does not exist'}])
    mock_tool.ainvoke = AsyncMock(return_value=payload)
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
