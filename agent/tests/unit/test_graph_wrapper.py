"""Unit tests for the graph telemetry context wrapper."""

import asyncio
import inspect
import sys
from unittest.mock import MagicMock

# Mock langchain_mcp_adapters to allow import of agent_core.tools during collection
if "langchain_mcp_adapters" not in sys.modules:
    mcp_mock = MagicMock()
    sys.modules["langchain_mcp_adapters"] = mcp_mock
    sys.modules["langchain_mcp_adapters.client"] = mcp_mock

import pytest
from agent_core.graph import with_telemetry_context


def test_sync_node_returning_dict():
    """Test that a synchronous node returning a dict works correctly."""

    def sync_node(state):
        return {"result": "sync"}

    wrapped = with_telemetry_context(sync_node)
    assert inspect.iscoroutinefunction(wrapped)

    result = asyncio.run(wrapped({}))
    assert result == {"result": "sync"}


@pytest.mark.asyncio
async def test_async_node_returning_dict():
    """Test that an asynchronous node returning a dict works correctly."""

    async def async_node(state):
        return {"result": "async"}

    wrapped = with_telemetry_context(async_node)
    result = await wrapped({})
    assert result == {"result": "async"}


@pytest.mark.asyncio
async def test_sync_node_returning_awaitable():
    """Test that a synchronous node returning an awaitable is awaited correctly."""

    async def some_coro():
        return {"result": "awaitable"}

    def sync_node(state):
        return some_coro()

    wrapped = with_telemetry_context(sync_node)
    result = await wrapped({})
    assert result == {"result": "awaitable"}


@pytest.mark.asyncio
async def test_sync_node_returning_none():
    """Test that a synchronous node returning None works correctly."""

    def sync_node(state):
        return None

    wrapped = with_telemetry_context(sync_node)
    result = await wrapped({})
    assert result is None
