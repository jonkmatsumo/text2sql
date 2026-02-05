"""Tests for persistence reliability in agent workflow.

Tests fail-loud behavior for create_interaction and update_interaction.
"""

import os
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


class TestCreateInteractionFailure:
    """Tests for create_interaction failure handling."""

    @pytest.mark.asyncio
    async def test_create_interaction_failure_raises_by_default(self):
        """Test that create_interaction failure raises exception (fail-closed default)."""
        import sys

        # Clean up polluted modules
        clean_modules = [m for m in sys.modules if m.startswith("agent")]
        for m in clean_modules:
            if not isinstance(sys.modules[m], type(sys)):
                del sys.modules[m]

        from agent.graph import run_agent_with_tracing

        # Mock create_interaction tool that raises
        mock_create_tool = AsyncMock()
        mock_create_tool.name = "create_interaction"
        mock_create_tool.ainvoke.side_effect = RuntimeError("DB connection failed")

        mock_update_tool = AsyncMock()
        mock_update_tool.name = "update_interaction"

        mock_tools = [mock_create_tool, mock_update_tool]

        # Strict mode should raise
        with patch.dict(os.environ, {"AGENT_INTERACTION_PERSISTENCE_MODE": "strict"}, clear=False):
            with (
                patch("agent.tools.get_mcp_tools", new=AsyncMock(return_value=mock_tools)),
                patch("agent.graph.telemetry") as mock_telemetry,
            ):
                # Properly configure telemetry mock to return expected types
                mock_telemetry.get_current_trace_id.return_value = None
                mock_telemetry.get_current_span.return_value = None

                # Should raise RuntimeError because default is fail-closed
                with pytest.raises(RuntimeError) as exc_info:
                    await run_agent_with_tracing("My question")

                assert "Interaction creation failed" in str(exc_info.value)
                assert "DB connection failed" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_create_interaction_failure_continues_in_fail_open_mode(self):
        """Test that create_interaction failure continues when PERSISTENCE_FAIL_OPEN=true."""
        import sys

        # Clean up polluted modules
        clean_modules = [m for m in sys.modules if m.startswith("agent")]
        for m in clean_modules:
            if not isinstance(sys.modules[m], type(sys)):
                del sys.modules[m]

        from agent.graph import run_agent_with_tracing

        # Mock create_interaction tool that raises
        mock_create_tool = AsyncMock()
        mock_create_tool.name = "create_interaction"
        mock_create_tool.ainvoke.side_effect = RuntimeError("DB connection failed")

        mock_update_tool = AsyncMock()
        mock_update_tool.name = "update_interaction"
        mock_update_tool.ainvoke.return_value = "OK"

        mock_tools = [mock_create_tool, mock_update_tool]

        # Mock app to return success
        mock_app = AsyncMock()
        mock_app.ainvoke.return_value = {
            "messages": [MagicMock(content="Hello World")],
            "current_sql": "SELECT 1",
            "error": None,
        }

        # Set best-effort mode
        with patch.dict(
            os.environ, {"AGENT_INTERACTION_PERSISTENCE_MODE": "best_effort"}, clear=False
        ):
            with (
                patch("agent.tools.get_mcp_tools", new=AsyncMock(return_value=mock_tools)),
                patch("agent.graph.app", mock_app),
                patch("agent.graph.telemetry") as mock_telemetry,
            ):
                # Properly configure telemetry mock to return expected types
                mock_telemetry.get_current_trace_id.return_value = None
                mock_telemetry.get_current_span.return_value = None

                # Should NOT raise - continues with interaction_id=None
                result = await run_agent_with_tracing("My question")

                # Workflow should complete
                assert result.get("current_sql") == "SELECT 1"
                # interaction_id should be None
                assert result.get("interaction_id") is None
                assert result.get("interaction_persisted") is False

    @pytest.mark.asyncio
    async def test_create_interaction_failure_logs_structured_error(self):
        """Test that create_interaction failure logs structured error."""
        import sys

        # Clean up polluted modules
        clean_modules = [m for m in sys.modules if m.startswith("agent")]
        for m in clean_modules:
            if not isinstance(sys.modules[m], type(sys)):
                del sys.modules[m]

        from agent.graph import run_agent_with_tracing

        # Mock create_interaction tool that raises
        mock_create_tool = AsyncMock()
        mock_create_tool.name = "create_interaction"
        mock_create_tool.ainvoke.side_effect = RuntimeError("DB connection failed")

        mock_tools = [mock_create_tool]

        # Enable best-effort so we can capture logging
        with patch.dict(
            os.environ, {"AGENT_INTERACTION_PERSISTENCE_MODE": "best_effort"}, clear=False
        ):
            with (
                patch("agent.tools.get_mcp_tools", new=AsyncMock(return_value=mock_tools)),
                patch("agent.graph.telemetry") as mock_telemetry,
                patch("agent.graph.logger") as mock_logger,
                patch("agent.graph.app", AsyncMock(return_value={"messages": [], "error": None})),
            ):
                # Properly configure telemetry mock to return expected types
                mock_telemetry.get_current_trace_id.return_value = None
                mock_telemetry.get_current_span.return_value = None

                await run_agent_with_tracing("My question")

                # Verify structured error was logged
                mock_logger.error.assert_called_once()
                call_args = mock_logger.error.call_args
                assert "Failed to create interaction" in call_args[0][0]
                assert call_args[1]["extra"]["operation"] == "create_interaction"
                assert call_args[1]["extra"]["exception_type"] == "RuntimeError"
