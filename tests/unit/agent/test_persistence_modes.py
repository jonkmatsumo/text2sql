"""Tests for interaction persistence modes."""

import os
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


@pytest.mark.asyncio
async def test_best_effort_mode_marks_persistence_failure():
    """Best-effort mode continues and marks interaction_persisted false."""
    from agent.graph import run_agent_with_tracing

    mock_create_tool = AsyncMock()
    mock_create_tool.name = "create_interaction"
    mock_create_tool.ainvoke.side_effect = RuntimeError("DB connection failed")

    mock_tools = [mock_create_tool]

    mock_app = AsyncMock()
    mock_app.ainvoke.return_value = {
        "messages": [MagicMock(content="ok")],
        "current_sql": "SELECT 1",
        "error": None,
    }

    with patch.dict(os.environ, {"AGENT_INTERACTION_PERSISTENCE_MODE": "best_effort"}, clear=False):
        with (
            patch("agent.tools.get_mcp_tools", new=AsyncMock(return_value=mock_tools)),
            patch("agent.graph.app", mock_app),
            patch("agent.graph.telemetry") as mock_telemetry,
        ):
            mock_telemetry.get_current_trace_id.return_value = None
            mock_telemetry.get_current_span.return_value = None

            result = await run_agent_with_tracing("My question")
            assert result.get("interaction_persisted") is False


@pytest.mark.asyncio
async def test_strict_mode_raises_on_persistence_failure():
    """Strict mode raises on interaction persistence failure."""
    from agent.graph import run_agent_with_tracing

    mock_create_tool = AsyncMock()
    mock_create_tool.name = "create_interaction"
    mock_create_tool.ainvoke.side_effect = RuntimeError("DB connection failed")

    mock_tools = [mock_create_tool]

    with patch.dict(os.environ, {"AGENT_INTERACTION_PERSISTENCE_MODE": "strict"}, clear=False):
        with (
            patch("agent.tools.get_mcp_tools", new=AsyncMock(return_value=mock_tools)),
            patch("agent.graph.telemetry") as mock_telemetry,
        ):
            mock_telemetry.get_current_trace_id.return_value = None
            mock_telemetry.get_current_span.return_value = None

            with pytest.raises(RuntimeError):
                await run_agent_with_tracing("My question")
