"""Pytest configuration and fixtures for agent tests."""

import os

# Set dummy API keys for CI/CD compatibility to prevent instantiation errors
# when validation checks for keys.
os.environ.setdefault("OPENAI_API_KEY", "sk-dummy-key-for-testing")
os.environ.setdefault("ANTHROPIC_API_KEY", "dummy-key")

try:
    from langchain_core.messages import AIMessage, BaseMessage, HumanMessage, ToolMessage
except ImportError:
    # If dependencies are not installed, we might need mocks,
    # but strictly speaking dependencies should be present.
    # We'll define simple classes if imports fail, though in this env they exist.
    class BaseMessage:
        """Mock BaseMessage."""

        def __init__(self, content):
            """Initialize with content."""
            self.content = content

    class HumanMessage(BaseMessage):
        """Mock HumanMessage."""

    class AIMessage(BaseMessage):
        """Mock AIMessage."""

    class ToolMessage(BaseMessage):
        """Mock ToolMessage."""

        def __init__(self, content, tool_call_id="1"):
            """Initialize with content and tool_call_id."""
            super().__init__(content)
            self.tool_call_id = tool_call_id


def pytest_configure(config):
    """Configure pytest - path management handled by root conftest.py."""
    # Path management is handled by root-level conftest.py
    # which ensures mcp-server is in path before agent
    # This conftest only handles agent-specific mocks and fixtures
    pass
