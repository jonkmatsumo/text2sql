"""Tests for interaction persistence circuit breaker."""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from agent.graph import run_agent_with_tracing


@pytest.mark.asyncio
async def test_run_agent_create_interaction_timeout(monkeypatch):
    """Test that agent fails open when interaction creation times out."""
    monkeypatch.setenv("AGENT_INTERACTION_PERSISTENCE_TIMEOUT_MS", "10")
    monkeypatch.setenv("AGENT_INTERACTION_PERSISTENCE_FAIL_OPEN", "True")

    async def slow_create(*args, **kwargs):
        await asyncio.sleep(0.1)
        return {"id": "int_123"}

    mock_create_tool = MagicMock()
    mock_create_tool.name = "create_interaction"
    mock_create_tool.ainvoke = AsyncMock(side_effect=slow_create)

    mock_tools = [mock_create_tool]

    with (
        patch("agent.tools.mcp_tools_context") as mock_mcp_ctx,
        patch("agent.graph.app.ainvoke", AsyncMock(return_value={"messages": []})),
    ):
        mock_mcp_ctx.return_value.__aenter__.return_value = mock_tools

        result = await run_agent_with_tracing(question="test", tenant_id=1)

        assert "interaction_id" not in result or result["interaction_id"] is None
        assert result["interaction_persisted"] is False


@pytest.mark.asyncio
async def test_run_agent_create_interaction_strict_mode(monkeypatch):
    """Test that agent fails when interaction creation times out in strict mode."""
    monkeypatch.setenv("AGENT_INTERACTION_PERSISTENCE_TIMEOUT_MS", "10")
    monkeypatch.setenv("AGENT_INTERACTION_PERSISTENCE_FAIL_OPEN", "False")
    monkeypatch.setenv("AGENT_INTERACTION_PERSISTENCE_MODE", "strict")

    async def slow_create(*args, **kwargs):
        await asyncio.sleep(0.1)
        return {"id": "int_123"}

    mock_create_tool = MagicMock()
    mock_create_tool.name = "create_interaction"
    mock_create_tool.ainvoke = AsyncMock(side_effect=slow_create)

    mock_tools = [mock_create_tool]

    with (
        patch("agent.tools.mcp_tools_context") as mock_mcp_ctx,
        patch("agent.graph.app.ainvoke", AsyncMock(return_value={"messages": []})),
    ):
        mock_mcp_ctx.return_value.__aenter__.return_value = mock_tools

        with pytest.raises(RuntimeError) as excinfo:
            await run_agent_with_tracing(question="test", tenant_id=1)
        assert "mode=strict" in str(excinfo.value)
