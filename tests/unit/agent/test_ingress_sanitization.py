from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, patch

import pytest

import agent.graph as graph_mod


@pytest.mark.xfail(reason="Fails in full suite due to pollution (passes isolated)")
@asynccontextmanager
async def mock_mcp_context():
    """Mock MCP context that yields empty tools list."""
    yield []


@pytest.mark.asyncio
async def test_run_agent_requires_tenant_id():
    """Agent entrypoint should reject missing tenant_id deterministically."""
    with pytest.raises(ValueError, match="tenant_id is required"):
        await graph_mod.run_agent_with_tracing("show me all tables", tenant_id=None)

    with pytest.raises(TypeError, match="tenant_id"):
        await graph_mod.run_agent_with_tracing("show me all tables")


@pytest.mark.asyncio
async def test_ingress_sanitization_applied():
    """Assert user input is sanitized at ingress."""
    # Input with trailing spaces and mixed case
    raw_input = "  SELECT * FROM users;  "
    # The common.sanitization will trim and lowercase it to "select * from users;"

    # Let's use a "dirty" but valid-ish input
    raw_input = "  What is the TOTAL revenue?  "
    expected_sanitized = "what is the total revenue?"

    with (
        patch("common.sanitization.sanitize_text") as mock_sanitize,
        patch.object(graph_mod, "app") as mock_app,
    ):

        # Mock sanitize_text to track calls
        from common.sanitization import SanitizationResult

        mock_sanitize.return_value = SanitizationResult(
            sanitized=expected_sanitized, is_valid=True, errors=[]
        )

        mock_app.ainvoke = AsyncMock(return_value={"messages": [], "raw_user_input": raw_input})

        with patch("agent.tools.mcp_tools_context", side_effect=mock_mcp_context):
            await graph_mod.run_agent_with_tracing(raw_input, tenant_id=1)

        # 1. Assert sanitization invoked exactly once
        assert mock_sanitize.call_count == 1
        assert mock_sanitize.call_args[0][0] == raw_input

        # 2. Assert downstream ainvoke received sanitized input
        # It's passed as first message content
        assert mock_app.ainvoke.called
        inputs = mock_app.ainvoke.call_args[0][0]
        human_msg = inputs["messages"][0]
        assert human_msg.content == expected_sanitized

        # 3. Assert raw input is stored in state
        assert inputs["raw_user_input"] == raw_input


@pytest.mark.asyncio
async def test_normal_input_behavior():
    """Confirm normal inputs do not change behavior."""
    normal_input = "show me all tables"

    # Patch the app object that run_agent_with_tracing actually uses
    with patch.object(graph_mod, "app") as mock_app:
        # It needs to be an AsyncMock because it's awaited
        mock_app.ainvoke = AsyncMock(return_value={"messages": []})

        # We also need to mock the tools context to avoid MCP connection attempts
        with patch("agent.tools.mcp_tools_context", side_effect=mock_mcp_context):
            await graph_mod.run_agent_with_tracing(normal_input, tenant_id=1)

        # Verify calls on our mock
        assert mock_app.ainvoke.called
        inputs = mock_app.ainvoke.call_args[0][0]
        assert inputs["messages"][0].content == "show me all tables"
