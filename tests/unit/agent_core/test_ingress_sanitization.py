from contextlib import asynccontextmanager
from unittest.mock import MagicMock, patch

import pytest

from agent_core.graph import run_agent_with_tracing


@asynccontextmanager
async def mock_mcp_context():
    """Mock MCP context that yields empty tools list."""
    yield []


@pytest.mark.asyncio
async def test_ingress_sanitization_applied():
    """Assert user input is sanitized at ingress."""
    # Input with trailing spaces and mixed case
    raw_input = "  SELECT * FROM users;  "
    # The common.sanitization will trim and lowercase it to "select * from users;"
    # Actually wait, select * from users; contains * and ;
    # * is in REGEX_META_CHARS. ; is NOT in allowlist?
    # ALLOWED_CHARS_PATTERN = re.compile(r"^[a-zA-Z0-9\s\-_/&'+\.\(\)]+$")
    # ; is NOT allowed.

    # Let's use a "dirty" but valid-ish input
    raw_input = "  What is the TOTAL revenue?  "
    expected_sanitized = "what is the total revenue?"

    with patch("common.sanitization.sanitize_text") as mock_sanitize, patch(
        "langgraph.graph.StateGraph.compile"
    ) as mock_compile:

        # Mock sanitize_text to track calls
        from common.sanitization import SanitizationResult

        mock_sanitize.return_value = SanitizationResult(
            sanitized=expected_sanitized, is_valid=True, errors=[]
        )

        # Mock the app (compiled graph)
        mock_app = MagicMock()
        mock_app.ainvoke.return_value = {"messages": [], "raw_user_input": raw_input}
        mock_compile.return_value = mock_app

        # We need to re-import or use the app from graph

        with patch("agent_core.graph.app", mock_app), patch(
            "agent_core.tools.mcp_tools_context", side_effect=mock_mcp_context
        ):
            await run_agent_with_tracing(raw_input)

        # 1. Assert sanitization invoked exactly once
        assert mock_sanitize.call_count == 1
        assert mock_sanitize.call_args[0][0] == raw_input

        # 2. Assert downstream ainvoke received sanitized input
        # It's passed as first message content
        inputs = mock_app.ainvoke.call_args[0][0]
        human_msg = inputs["messages"][0]
        assert human_msg.content == expected_sanitized

        # 3. Assert raw input is stored in state
        assert inputs["raw_user_input"] == raw_input


@pytest.mark.asyncio
async def test_normal_input_behavior():
    """Confirm normal inputs do not change behavior."""
    normal_input = "show me all tables"

    # We won't mock sanitize_text here to use the real one
    with patch("langgraph.graph.StateGraph.compile") as mock_compile:
        mock_app = MagicMock()
        mock_app.ainvoke.return_value = {"messages": []}
        mock_compile.return_value = mock_app

        with patch("agent_core.graph.app", mock_app), patch(
            "agent_core.tools.mcp_tools_context", side_effect=mock_mcp_context
        ):
            await run_agent_with_tracing(normal_input)

        inputs = mock_app.ainvoke.call_args[0][0]
        assert inputs["messages"][0].content == "show me all tables"
