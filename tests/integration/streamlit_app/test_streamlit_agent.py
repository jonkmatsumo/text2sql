"""Unit tests for Streamlit AgentService.

NOTE:
Renamed from test_agent.py to avoid pytest import collisions with
agent/test_agent.py during repo-root test collection.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest


@pytest.fixture(autouse=True)
def mock_agent_dependencies():
    """Mock agent dependencies for each test to avoid state leakage."""
    mcp_mock = MagicMock()
    mcp_mock.__path__ = []
    mocks = {
        "mcp": mcp_mock,
        "mcp.types": MagicMock(),
        "mcp.client": MagicMock(),
        "mcp.client.sse": MagicMock(),
        "mcp.client.streamable_http": MagicMock(),
        "mlflow": MagicMock(),
        "agent_core": MagicMock(),
        "agent_core.graph": MagicMock(),
        "agent_core.telemetry": MagicMock(),
        "agent_core.state": MagicMock(),
        "agent_core.nodes": MagicMock(),
        "agent_core.tools": MagicMock(),
    }
    with patch.dict("sys.modules", mocks):
        yield mocks


# We import AgentService here but it uses deferred imports internally
# for things that would be mocked by the fixture above.
from streamlit_app.service.agent import AgentService  # noqa: E402


class TestRunAgent:
    """Tests for run_agent function."""

    @pytest.mark.asyncio
    async def test_run_agent_success(self, mock_agent_dependencies):
        """Test successful agent execution."""
        mocks = mock_agent_dependencies
        mock_state = {
            "current_sql": "SELECT COUNT(*) FROM films",
            "query_result": [{"count": 1000}],
            "messages": [MagicMock(content="Question"), MagicMock(content="There are 1000 films.")],
            "error": None,
            "from_cache": False,
            "interaction_id": "int-123",
            "viz_spec": {"mark": "bar"},
            "viz_reason": "Data good",
        }

        # Use the mock from the fixture
        mock_graph = mocks["agent_core.graph"]
        mock_run = AsyncMock(return_value=mock_state)
        mock_graph.run_agent_with_tracing = mock_run

        results = await AgentService.run_agent("How many films?", tenant_id=1)

        assert results["sql"] == "SELECT COUNT(*) FROM films"
        assert results["result"] == [{"count": 1000}]
        assert results["response"] == "There are 1000 films."
        assert results["interaction_id"] == "int-123"
        assert results["viz_spec"] == {"mark": "bar"}
        assert results["viz_reason"] == "Data good"


class TestFeedback:
    """Tests for feedback submission."""

    @pytest.mark.asyncio
    async def test_submit_feedback_success(self, mock_agent_dependencies):
        """Test successful feedback submission."""
        mocks = mock_agent_dependencies
        mock_tool = AsyncMock()
        mock_tool.name = "submit_feedback"

        # Use the mock from the fixture
        mock_tools_module = mocks["agent_core.tools"]
        mock_get_tools = AsyncMock(return_value=[mock_tool])
        mock_tools_module.get_mcp_tools = mock_get_tools

        success = await AgentService.submit_feedback("int-123", "UP", "Great")

        assert success is True
        mock_tool.ainvoke.assert_called_once()
        args = mock_tool.ainvoke.call_args[0][0]
        assert args["interaction_id"] == "int-123"
        assert args["thumb"] == "UP"

    @pytest.mark.asyncio
    async def test_submit_feedback_tool_missing(self, mock_agent_dependencies):
        """Test feedback submission when tool is missing."""
        mocks = mock_agent_dependencies

        # Mock tools list WITHOUT submit_feedback
        mock_other_tool = MagicMock()
        mock_other_tool.name = "other_tool"

        mock_tools_module = mocks["agent_core.tools"]
        mock_get_tools = AsyncMock(return_value=[mock_other_tool])
        mock_tools_module.get_mcp_tools = mock_get_tools

        with patch("streamlit_app.service.agent.logger") as mock_logger:
            success = await AgentService.submit_feedback("int-123", "UP", "Great")

            assert success is False
            mock_logger.error.assert_called_once()
            assert "not found in MCP tools list" in mock_logger.error.call_args[0][0]


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
            "from_cache": False,
            "interaction_id": "int-123",
            "viz_spec": {"mark": "bar"},
            "viz_reason": "Reason",
        }

        entry = AgentService.format_conversation_entry("Show films", results)

        assert entry["question"] == "Show films"
        assert entry["interaction_id"] == "int-123"
        assert entry["viz_spec"] == {"mark": "bar"}
        assert entry["viz_reason"] == "Reason"


class TestValidateTenantId:
    """Tests for validate_tenant_id function."""

    def test_valid_tenant_id(self):
        """Test with valid tenant ID."""
        assert AgentService.validate_tenant_id(5) == 5
