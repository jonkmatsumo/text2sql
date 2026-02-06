"""Tests for cache_lookup node exception handling."""

from unittest.mock import AsyncMock, MagicMock

import pytest

from agent.nodes.cache_lookup import cache_lookup_node
from agent.state import AgentState


@pytest.fixture
def mock_logger(monkeypatch):
    """Mock the logger."""
    mock = MagicMock()
    monkeypatch.setattr("agent.nodes.cache_lookup.logger", mock)
    return mock


@pytest.fixture
def mock_tools(monkeypatch):
    """Mock the MCP tools."""
    tools = [AsyncMock(name="lookup_cache"), AsyncMock(name="get_semantic_subgraph")]
    tools[0].name = "lookup_cache"
    tools[1].name = "get_semantic_subgraph"

    # get_mcp_tools is awaited
    mock_get = AsyncMock(return_value=tools)
    monkeypatch.setattr("agent.nodes.cache_lookup.get_mcp_tools", mock_get)
    return tools


async def test_cache_lookup_swallowed_exception(mock_tools, mock_logger):
    """Verify that exceptions in cache lookup are caught, logged structuredly, and return miss."""
    state = AgentState(messages=[], tenant_id=1)

    # Simulate tool exploding
    mock_tools[0].ainvoke.side_effect = RuntimeError("Boom")

    result = await cache_lookup_node(state)

    # Should not crash
    assert result["from_cache"] is False
    assert result["cached_sql"] is None

    # Verify structured logging
    mock_logger.error.assert_called_once()
    args, kwargs = mock_logger.error.call_args
    assert "Boom" in args[0]
    assert "extra" in kwargs
    assert kwargs["extra"]["error_type"] == "RuntimeError"
    assert kwargs["extra"]["error"] == "Boom"
