"""Pytest configuration and fixtures for agent tests."""

import sys
from unittest.mock import MagicMock


def pytest_configure(config):
    """Configure pytest - path management handled by root conftest.py."""
    # Path management is handled by root-level conftest.py
    # which ensures mcp-server is in path before agent
    # This conftest only handles agent-specific mocks and fixtures
    pass


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
sys.modules["langchain_openai"].ChatOpenAI = MagicMock
sys.modules["langchain_postgres"] = MagicMock()
sys.modules["langchain_postgres"].PGVector = MagicMock

# Mock langchain_mcp_adapters for MCP server connection
sys.modules["langchain_mcp_adapters"] = MagicMock()
sys.modules["langchain_mcp_adapters.client"] = MagicMock()
sys.modules["langchain_mcp_adapters.client"].MultiServerMCPClient = MagicMock

# Mock langchain_core.prompts for prompt templates
sys.modules["langchain_core.prompts"] = MagicMock()
sys.modules["langchain_core.prompts"].ChatPromptTemplate = MagicMock
