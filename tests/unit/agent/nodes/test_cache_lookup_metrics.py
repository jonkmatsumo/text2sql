"""Metrics regression tests for cache_lookup node."""

from unittest.mock import AsyncMock, patch

import pytest
from langchain_core.messages import HumanMessage

from agent.nodes.cache_lookup import cache_lookup_node


@pytest.mark.asyncio
async def test_cache_lookup_hit_emits_hit_metric(monkeypatch):
    """Cache hit path should emit outcome=hit metric."""
    monkeypatch.setenv("AGENT_CACHE_SCHEMA_VALIDATION", "false")
    tool = AsyncMock()
    tool.name = "lookup_cache"
    tool.ainvoke = AsyncMock(return_value={})

    with (
        patch("agent.nodes.cache_lookup.get_mcp_tools", AsyncMock(return_value=[tool])),
        patch(
            "agent.nodes.cache_lookup.parse_tool_output",
            return_value=[
                {
                    "value": "SELECT 1",
                    "cache_id": "cache-1",
                    "similarity": 0.99,
                    "metadata": {"schema_snapshot_id": "snap-1"},
                }
            ],
        ),
        patch("agent.nodes.cache_lookup.unwrap_envelope", side_effect=lambda value: value),
        patch("agent.nodes.cache_lookup.agent_metrics.add_counter") as mock_add_counter,
    ):
        state = {"messages": [HumanMessage(content="show users")], "tenant_id": 1}
        result = await cache_lookup_node(state)

    assert result["from_cache"] is True
    mock_add_counter.assert_any_call(
        "agent.cache.lookup_total",
        attributes={"outcome": "hit"},
        description="Cache lookup outcomes (hit/miss/error)",
    )


@pytest.mark.asyncio
async def test_cache_lookup_miss_emits_miss_metric(monkeypatch):
    """Cache miss path should emit outcome=miss metric."""
    monkeypatch.setenv("AGENT_CACHE_SCHEMA_VALIDATION", "false")
    tool = AsyncMock()
    tool.name = "lookup_cache"
    tool.ainvoke = AsyncMock(return_value={})

    with (
        patch("agent.nodes.cache_lookup.get_mcp_tools", AsyncMock(return_value=[tool])),
        patch("agent.nodes.cache_lookup.parse_tool_output", return_value=[]),
        patch("agent.nodes.cache_lookup.agent_metrics.add_counter") as mock_add_counter,
    ):
        state = {"messages": [HumanMessage(content="show users")], "tenant_id": 1}
        result = await cache_lookup_node(state)

    assert result["from_cache"] is False
    mock_add_counter.assert_any_call(
        "agent.cache.lookup_total",
        attributes={"outcome": "miss"},
        description="Cache lookup outcomes (hit/miss/error)",
    )
