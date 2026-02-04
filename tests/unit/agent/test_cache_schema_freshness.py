"""Tests for cache-hit schema freshness gate."""

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from langchain_core.messages import HumanMessage

from agent.nodes.cache_lookup import cache_lookup_node
from agent.state import AgentState


def _mock_span_ctx(mock_start_span):
    mock_span = MagicMock()
    mock_start_span.return_value.__enter__ = MagicMock(return_value=mock_span)
    mock_start_span.return_value.__exit__ = MagicMock(return_value=False)
    return mock_span


@pytest.mark.asyncio
@patch("agent.nodes.cache_lookup.telemetry.start_span")
@patch("agent.nodes.cache_lookup.get_mcp_tools")
async def test_cache_hit_with_matching_snapshot_allows_cache(mock_get_tools, mock_start_span):
    """Matching schema snapshot should allow cache hit."""
    _mock_span_ctx(mock_start_span)

    cache_tool = AsyncMock()
    cache_tool.name = "lookup_cache"
    cache_tool.ainvoke = AsyncMock(
        return_value=json.dumps(
            {
                "cache_id": "cache-1",
                "value": "SELECT 1",
                "similarity": 1.0,
                "metadata": {"schema_snapshot_id": "fp-abc"},
            }
        )
    )

    subgraph_tool = AsyncMock()
    subgraph_tool.name = "get_semantic_subgraph"
    subgraph_tool.ainvoke = AsyncMock(
        return_value=json.dumps({"nodes": [{"type": "Table", "name": "t1"}]})
    )

    mock_get_tools.return_value = [cache_tool, subgraph_tool]

    state = AgentState(
        messages=[HumanMessage(content="Show data")],
        schema_context="",
        current_sql=None,
        query_result=None,
        error=None,
        retry_count=0,
        tenant_id=1,
    )

    with (
        patch("agent.nodes.cache_lookup.get_env_bool", return_value=True),
        patch("agent.utils.schema_fingerprint.resolve_schema_snapshot_id", return_value="fp-abc"),
    ):
        result = await cache_lookup_node(state)

    assert result["from_cache"] is True
    assert result["current_sql"] == "SELECT 1"


@pytest.mark.asyncio
@patch("agent.nodes.cache_lookup.telemetry.start_span")
@patch("agent.nodes.cache_lookup.get_mcp_tools")
async def test_cache_hit_with_mismatched_snapshot_rejected(mock_get_tools, mock_start_span):
    """Mismatched schema snapshot should reject cache hit."""
    _mock_span_ctx(mock_start_span)

    cache_tool = AsyncMock()
    cache_tool.name = "lookup_cache"
    cache_tool.ainvoke = AsyncMock(
        return_value=json.dumps(
            {
                "cache_id": "cache-1",
                "value": "SELECT 1",
                "similarity": 1.0,
                "metadata": {"schema_snapshot_id": "fp-old"},
            }
        )
    )

    subgraph_tool = AsyncMock()
    subgraph_tool.name = "get_semantic_subgraph"
    subgraph_tool.ainvoke = AsyncMock(
        return_value=json.dumps({"nodes": [{"type": "Table", "name": "t2"}]})
    )

    mock_get_tools.return_value = [cache_tool, subgraph_tool]

    state = AgentState(
        messages=[HumanMessage(content="Show data")],
        schema_context="",
        current_sql=None,
        query_result=None,
        error=None,
        retry_count=0,
        tenant_id=1,
    )

    with (
        patch("agent.nodes.cache_lookup.get_env_bool", return_value=True),
        patch("agent.utils.schema_fingerprint.resolve_schema_snapshot_id", return_value="fp-new"),
    ):
        result = await cache_lookup_node(state)

    assert result["from_cache"] is False
    assert result["cached_sql"] is None
    assert result["rejected_cache_context"]["reason"] == "schema_snapshot_mismatch"


@pytest.mark.asyncio
@patch("agent.nodes.cache_lookup.telemetry.start_span")
@patch("agent.nodes.cache_lookup.get_mcp_tools")
async def test_cache_hit_without_snapshot_allowed(mock_get_tools, mock_start_span):
    """Missing cached snapshot should allow cache hit."""
    _mock_span_ctx(mock_start_span)

    cache_tool = AsyncMock()
    cache_tool.name = "lookup_cache"
    cache_tool.ainvoke = AsyncMock(
        return_value=json.dumps({"cache_id": "cache-1", "value": "SELECT 1", "similarity": 1.0})
    )

    mock_get_tools.return_value = [cache_tool]

    state = AgentState(
        messages=[HumanMessage(content="Show data")],
        schema_context="",
        current_sql=None,
        query_result=None,
        error=None,
        retry_count=0,
        tenant_id=1,
    )

    with patch("agent.nodes.cache_lookup.get_env_bool", return_value=True):
        result = await cache_lookup_node(state)

    assert result["from_cache"] is True
    assert result["current_sql"] == "SELECT 1"
