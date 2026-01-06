"""Pytest configuration and fixtures for agent tests."""

import sys
from pathlib import Path
from unittest.mock import MagicMock


def pytest_configure(config):
    """Configure pytest - add agent directory to path for agent test imports."""
    # Add agent directory to Python path for 'from src' imports
    # This allows agent tests to import from src.state, etc.
    agent_dir = Path(__file__).parent.parent
    agent_dir_str = str(agent_dir.resolve())

    # Only add if not already present
    if agent_dir_str not in sys.path:
        # Find mcp-server in path and insert agent after it
        # This ensures mcp-server tests find mcp-server/src first
        mcp_server_idx = None
        for i, path in enumerate(sys.path):
            path_obj = Path(path)
            if path_obj.exists() and path_obj.name == "mcp-server":
                mcp_server_idx = i
                break

        if mcp_server_idx is not None:
            # Insert after mcp-server so mcp-server tests still find mcp-server/src
            sys.path.insert(mcp_server_idx + 1, agent_dir_str)
        else:
            # MCP-server not in path yet, add agent at beginning
            # It will be moved after mcp-server when mcp-server is added
            sys.path.insert(0, agent_dir_str)


# Mock langchain_core and langgraph modules before any imports
# This allows tests to run without installing the actual dependencies


# Create mock message classes
class MockBaseMessage:
    """Mock BaseMessage for testing."""

    def __init__(self, content: str):
        """Initialize mock message with content."""
        self.content = content


class MockHumanMessage(MockBaseMessage):
    """Mock HumanMessage for testing."""


class MockAIMessage(MockBaseMessage):
    """Mock AIMessage for testing."""


class MockToolMessage(MockBaseMessage):
    """Mock ToolMessage for testing."""

    def __init__(self, content: str, tool_call_id: str = "1"):
        """Initialize mock tool message with content and tool call ID."""
        super().__init__(content)
        self.tool_call_id = tool_call_id


# Mock add_messages reducer
def mock_add_messages(left, right):
    """Mock add_messages reducer that concatenates message lists."""
    return left + right


# Set up module mocks
sys.modules["langchain_core"] = MagicMock()
sys.modules["langchain_core.messages"] = MagicMock()
sys.modules["langchain_core.messages"].BaseMessage = MockBaseMessage
sys.modules["langchain_core.messages"].HumanMessage = MockHumanMessage
sys.modules["langchain_core.messages"].AIMessage = MockAIMessage
sys.modules["langchain_core.messages"].ToolMessage = MockToolMessage

sys.modules["langgraph"] = MagicMock()
sys.modules["langgraph.graph"] = MagicMock()
sys.modules["langgraph.graph.message"] = MagicMock()
sys.modules["langgraph.graph.message"].add_messages = mock_add_messages

# Mock langchain_openai and langchain_postgres for agent tests
sys.modules["langchain_openai"] = MagicMock()
sys.modules["langchain_openai"].OpenAIEmbeddings = MagicMock
sys.modules["langchain_postgres"] = MagicMock()
sys.modules["langchain_postgres"].PGVector = MagicMock
