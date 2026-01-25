from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# Import the function to test
from agent.graph import run_agent_with_tracing


@pytest.mark.asyncio
async def test_create_interaction_receives_config():
    """Verify that create_interaction tool invocation receives the config object."""

    async def mock_retry(func, *args, **kwargs):
        return await func()

    # Mock dependencies
    with (
        patch("agent.tools.mcp_tools_context") as mock_mcp_ctx,
        patch("agent.telemetry.telemetry") as mock_telemetry,
        patch("agent.graph.app") as mock_app,
        patch("agent.utils.retry.retry_with_backoff", side_effect=mock_retry),
    ):

        # Setup mock tools
        mock_create_tool = MagicMock()
        mock_create_tool.name = "create_interaction"
        mock_create_tool.ainvoke = AsyncMock(return_value={"id": "mock_id"})

        mock_update_tool = MagicMock()
        mock_update_tool.name = "update_interaction"
        mock_update_tool.ainvoke = AsyncMock(return_value={"status": "updated"})

        # Context manager yield
        mock_mcp_ctx.return_value.__aenter__.return_value = [mock_create_tool, mock_update_tool]
        mock_mcp_ctx.return_value.__aexit__.return_value = None

        # Mock telemetry context capture
        mock_telemetry.capture_context.return_value = {}
        mock_telemetry.serialize_context.return_value = "{}"
        mock_telemetry.get_current_span.return_value = MagicMock()

        # Mock workflow execution
        mock_app.ainvoke = AsyncMock(return_value={"messages": [], "error": None})

        # Run the function
        # sanitize_text is imported from common.sanitization inside
        with patch("common.sanitization.sanitize_text") as mock_sanitize:
            mock_sanitize.return_value.sanitized = "test question"

            with patch("agent.tools.unpack_mcp_result", return_value="interaction_123"):
                await run_agent_with_tracing("test question", thread_id="thread_123")

        # Assertions
        assert mock_create_tool.ainvoke.call_count == 1
        call_args = mock_create_tool.ainvoke.call_args

        assert len(call_args[0]) == 1
        assert "config" in call_args[1]
        config = call_args[1]["config"]
        assert config["configurable"].get("thread_id") == "thread_123"


@pytest.mark.asyncio
async def test_update_interaction_receives_config():
    """Verify that update_interaction tool invocation receives the config object."""

    async def mock_retry(func, *args, **kwargs):
        return await func()

    with (
        patch("agent.tools.mcp_tools_context") as mock_mcp_ctx,
        patch("agent.telemetry.telemetry") as mock_telemetry,
        patch("agent.graph.app") as mock_app,
        patch("agent.utils.retry.retry_with_backoff", side_effect=mock_retry),
    ):

        mock_create_tool = MagicMock()
        mock_create_tool.name = "create_interaction"
        mock_create_tool.ainvoke = AsyncMock(return_value={"id": "mock_id"})

        mock_update_tool = MagicMock()
        mock_update_tool.name = "update_interaction"
        mock_update_tool.ainvoke = AsyncMock(return_value={"status": "updated"})

        mock_mcp_ctx.return_value.__aenter__.return_value = [mock_create_tool, mock_update_tool]
        mock_mcp_ctx.return_value.__aexit__.return_value = None

        mock_telemetry.capture_context.return_value = {}
        mock_telemetry.serialize_context.return_value = "{}"
        mock_telemetry.get_current_span.return_value = MagicMock()

        mock_app.ainvoke = AsyncMock(return_value={"messages": [], "error": None})

        with patch("common.sanitization.sanitize_text") as mock_sanitize:
            mock_sanitize.return_value.sanitized = "test question"

            with patch("agent.tools.unpack_mcp_result", return_value="interaction_123"):
                await run_agent_with_tracing("test question", thread_id="thread_123")

        # Assertions for update tool
        if mock_update_tool.ainvoke.call_count > 0:
            call_args = mock_update_tool.ainvoke.call_args
            assert "config" in call_args[1]
            config = call_args[1]["config"]
            assert config["configurable"].get("thread_id") == "thread_123"
        else:
            pytest.fail("update_tool.ainvoke was not called")
