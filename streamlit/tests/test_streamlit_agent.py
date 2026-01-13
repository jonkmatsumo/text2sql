"""Unit tests for Streamlit AgentService.

NOTE:
Renamed from test_agent.py to avoid pytest import collisions with
agent/test_agent.py during repo-root test collection.
"""

import os
import sys
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# Set OpenAI API key before importing app_logic (which imports agent modules)
os.environ.setdefault("OPENAI_API_KEY", "test-key-for-testing-only")


# Mock missing dependencies
sys.modules["langchain_mcp_adapters"] = MagicMock()
sys.modules["langchain_mcp_adapters.client"] = MagicMock()
sys.modules["mlflow"] = MagicMock()
sys.modules["agent_core"] = MagicMock()
sys.modules["agent_core.graph"] = MagicMock()
sys.modules["agent_core.telemetry"] = MagicMock()
sys.modules["agent_core.state"] = MagicMock()
sys.modules["agent_core.nodes"] = MagicMock()
sys.modules["agent_core.tools"] = MagicMock()

from streamlit_app.service.agent import AgentService  # noqa: E402


class TestRunAgent:
    """Tests for run_agent function."""

    @pytest.mark.asyncio
    async def test_run_agent_success(self):
        """Test successful agent execution."""
        mock_state = {
            "current_sql": "SELECT COUNT(*) FROM films",
            "query_result": [{"count": 1000}],
            "messages": [MagicMock(content="Question"), MagicMock(content="There are 1000 films.")],
            "error": None,
            "from_cache": False,
            "interaction_id": "int-123",
        }

        with patch("agent_core.graph.run_agent_with_tracing", new_callable=AsyncMock) as mock_run:
            mock_run.return_value = mock_state

            results = await AgentService.run_agent("How many films?", tenant_id=1)

            assert results["sql"] == "SELECT COUNT(*) FROM films"
            assert results["result"] == [{"count": 1000}]
            assert results["response"] == "There are 1000 films."
            assert results["interaction_id"] == "int-123"


class TestFeedback:
    """Tests for feedback submission."""

    @pytest.mark.asyncio
    async def test_submit_feedback_success(self):
        """Test successful feedback submission."""
        mock_tool = AsyncMock()
        mock_tool.name = "submit_feedback_tool"

        with patch("agent_core.tools.get_mcp_tools", new_callable=AsyncMock) as mock_get_tools:
            mock_get_tools.return_value = [mock_tool]

            success = await AgentService.submit_feedback("int-123", "UP", "Great")

            assert success is True
            mock_tool.ainvoke.assert_called_once()
            args = mock_tool.ainvoke.call_args[0][0]
            assert args["interaction_id"] == "int-123"
            assert args["thumb"] == "UP"


class TestFormatConversationEntry:
    """Tests for format_conversation_entry function."""

    def test_format_successful_entry(self):
        """Test formatting successful conversation entry."""
        results = {
            "sql": "SELECT * FROM films",
            "result": [{"id": 1}],
            "response": "Found 1 film",
            "error": None,
            "from_cache": False,
            "interaction_id": "int-123",
        }

        entry = AgentService.format_conversation_entry("Show films", results)

        assert entry["question"] == "Show films"
        assert entry["interaction_id"] == "int-123"


class TestValidateTenantId:
    """Tests for validate_tenant_id function."""

    def test_valid_tenant_id(self):
        """Test with valid tenant ID."""
        assert AgentService.validate_tenant_id(5) == 5
