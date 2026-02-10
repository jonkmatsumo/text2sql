"""Unit tests for error sanitization in synthesize_insight_node."""

from unittest.mock import MagicMock

import pytest
from langchain_core.messages import HumanMessage

from agent.nodes.synthesize import synthesize_insight_node


@pytest.fixture
def mock_telemetry(monkeypatch):
    """Mock telemetry emission."""
    mock = MagicMock()
    monkeypatch.setattr("agent.nodes.synthesize.telemetry", mock)
    return mock


@pytest.fixture
def base_state():
    """Provide base AgentState for tests."""
    return {
        "messages": [HumanMessage(content="What are the top users?")],
        "query_result": None,
        "retry_count": 0,
        "schema_refresh_count": 0,
        "schema_context": "",
    }


def test_synthesize_redacts_credentials(mock_telemetry, base_state):
    """Verify that credentials in errors are redacted."""
    state = base_state.copy()
    state.update(
        {
            "error": "Failed to connect: postgresql://admin:secret123@localhost:5432/db",
            "error_category": "timeout",  # Safe category but needs redaction
        }
    )

    result = synthesize_insight_node(state)
    content = result["messages"][0].content

    assert "admin" not in content
    assert "secret123" not in content
    assert "<user>:<password>" in content


def test_synthesize_generic_message_for_unknown_error(mock_telemetry, base_state):
    """Verify that unknown error categories get a generic message."""
    state = base_state.copy()
    state.update({"error": "Table 'secret_schema_info' not found", "error_category": "unknown"})

    result = synthesize_insight_node(state)
    content = result["messages"][0].content

    assert "secret_schema_info" not in content
    assert "An internal error occurred while processing your request." in content


def test_synthesize_generic_message_for_syntax_error(mock_telemetry, base_state):
    """Verify that syntax errors (unsafe) get a generic message."""
    state = base_state.copy()
    state.update(
        {
            "error": 'SQL Syntax Error: relation "users_private" does not exist',
            "error_category": "syntax",
        }
    )

    result = synthesize_insight_node(state)
    content = result["messages"][0].content

    assert "users_private" not in content
    assert "An internal error occurred while processing your request." in content


def test_synthesize_safe_category_passthrough(mock_telemetry, base_state):
    """Verify that safe categories pass through the redacted error."""
    state = base_state.copy()
    state.update({"error": "Operation timed out after 30s", "error_category": "timeout"})

    result = synthesize_insight_node(state)
    content = result["messages"][0].content

    # Check if the error is contained within the message
    assert "Operation timed out after 30s" in content


def test_synthesize_unsupported_capability_specific(mock_telemetry, base_state):
    """Verify that unsupported_capability still gives specific feedback."""
    state = base_state.copy()
    state.update(
        {
            "error": "Unsupported",
            "error_category": "unsupported_capability",
            "error_metadata": {"required_capability": "recursive_queries"},
        }
    )

    result = synthesize_insight_node(state)
    content = result["messages"][0].content

    assert "recursive_queries" in content
    assert "The database backend does not support" in content


def test_synthesize_invalid_request_passthrough(mock_telemetry, base_state):
    """Verify that invalid_request (safe) passes through the redacted error."""
    state = base_state.copy()
    state.update(
        {
            "error": "Redshift validation failed: ARRAY syntax not supported",
            "error_category": "invalid_request",
        }
    )

    result = synthesize_insight_node(state)
    content = result["messages"][0].content

    assert "I encountered a validation error" in content
    assert "ARRAY syntax not supported" in content
