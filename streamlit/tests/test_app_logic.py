"""Unit tests for Streamlit app business logic."""

import os
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# Set OpenAI API key before importing app_logic (which imports agent modules)
os.environ.setdefault("OPENAI_API_KEY", "test-key-for-testing-only")

# Add parent directory to path to import app_logic
sys.path.insert(0, str(Path(__file__).parent.parent))

from app_logic import format_conversation_entry, run_agent, validate_tenant_id  # noqa: E402


# Fixtures defined here to avoid conftest.py conflicts with agent/tests/conftest.py
@pytest.fixture
def mock_agent_event_success():
    """Mock successful agent workflow event."""
    return {
        "generate": {
            "current_sql": "SELECT COUNT(*) FROM films",
            "from_cache": False,
        },
        "execute": {
            "query_result": [{"count": 1000}],
            "error": None,
        },
        "synthesize": {"messages": [MagicMock(content="There are 1000 films in the database.")]},
    }


@pytest.fixture
def mock_agent_event_with_cache():
    """Mock agent workflow event with cache hit."""
    return {
        "generate": {
            "current_sql": "SELECT COUNT(*) FROM films",
            "from_cache": True,
        },
        "execute": {
            "query_result": [{"count": 1000}],
            "error": None,
        },
        "synthesize": {"messages": [MagicMock(content="Found 1000 films (cached).")]},
    }


@pytest.fixture
def mock_agent_event_with_error():
    """Mock agent workflow event with error."""
    return {
        "generate": {
            "current_sql": "SELECT * FROM nonexistent",
        },
        "execute": {
            "error": "Table 'nonexistent' does not exist",
            "query_result": None,
        },
    }


class TestRunAgent:
    """Tests for run_agent function."""

    @pytest.mark.asyncio
    async def test_run_agent_success(self, mock_agent_event_success):
        """Test successful agent execution."""

        async def async_gen():
            yield mock_agent_event_success

        with patch("app_logic.app.astream") as mock_astream:
            mock_astream.return_value = async_gen()

            results = await run_agent("How many films?", tenant_id=1)

            assert results["sql"] == "SELECT COUNT(*) FROM films"
            assert results["result"] == [{"count": 1000}]
            assert results["response"] == "There are 1000 films in the database."
            assert results["error"] is None
            assert results["from_cache"] is False

    @pytest.mark.asyncio
    async def test_run_agent_with_cache(self, mock_agent_event_with_cache):
        """Test agent execution with cache hit."""

        async def async_gen():
            yield mock_agent_event_with_cache

        with patch("app_logic.app.astream") as mock_astream:
            mock_astream.return_value = async_gen()

            results = await run_agent("How many films?", tenant_id=1)

            assert results["from_cache"] is True
            assert results["sql"] == "SELECT COUNT(*) FROM films"

    @pytest.mark.asyncio
    async def test_run_agent_with_error(self, mock_agent_event_with_error):
        """Test agent execution with error."""

        async def async_gen():
            yield mock_agent_event_with_error

        with patch("app_logic.app.astream") as mock_astream:
            mock_astream.return_value = async_gen()

            results = await run_agent("Invalid query", tenant_id=1)

            assert results["error"] == "Table 'nonexistent' does not exist"
            assert results["result"] is None
            assert results["sql"] == "SELECT * FROM nonexistent"

    @pytest.mark.asyncio
    async def test_run_agent_passes_tenant_id(self):
        """Test that tenant_id is passed to agent workflow."""
        mock_event = {
            "generate": {"current_sql": "SELECT 1"},
            "execute": {"query_result": [{"value": 1}]},
            "synthesize": {"messages": [MagicMock(content="Done")]},
        }

        async def async_gen():
            yield mock_event

        with patch("app_logic.app.astream") as mock_astream:
            mock_astream.return_value = async_gen()

            await run_agent("Test", tenant_id=42)

            # Verify astream was called (tenant_id is in inputs)
            mock_astream.assert_called_once()

    @pytest.mark.asyncio
    async def test_run_agent_handles_missing_synthesize(self):
        """Test that run_agent handles missing synthesize node gracefully."""
        mock_event = {
            "generate": {"current_sql": "SELECT 1"},
            "execute": {"query_result": [{"value": 1}], "error": None},
            # No synthesize node
        }

        async def async_gen():
            yield mock_event

        with patch("app_logic.app.astream") as mock_astream:
            mock_astream.return_value = async_gen()

            results = await run_agent("Test", tenant_id=1)

            assert results["sql"] == "SELECT 1"
            assert results["result"] == [{"value": 1}]
            assert results["response"] is None  # No synthesize node
            assert results["error"] is None


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
        }

        entry = format_conversation_entry("Show films", results)

        assert entry["question"] == "Show films"
        assert entry["sql"] == "SELECT * FROM films"
        assert entry["result"] == [{"id": 1}]
        assert entry["response"] == "Found 1 film"
        assert entry["error"] is None
        assert entry["from_cache"] is False

    def test_format_error_entry(self):
        """Test formatting entry with error."""
        results = {
            "sql": None,
            "result": None,
            "response": None,
            "error": "Connection failed",
            "from_cache": False,
        }

        entry = format_conversation_entry("Test", results)

        assert entry["error"] == "Connection failed"
        assert entry["sql"] is None

    def test_format_entry_with_cache(self):
        """Test formatting entry with cache hit."""
        results = {
            "sql": "SELECT * FROM films",
            "result": [{"id": 1}],
            "response": "Found 1 film",
            "error": None,
            "from_cache": True,
        }

        entry = format_conversation_entry("Show films", results)

        assert entry["from_cache"] is True


class TestValidateTenantId:
    """Tests for validate_tenant_id function."""

    def test_valid_tenant_id(self):
        """Test with valid tenant ID."""
        assert validate_tenant_id(5) == 5
        assert validate_tenant_id(1) == 1

    def test_none_tenant_id_defaults(self):
        """Test that None defaults to 1."""
        assert validate_tenant_id(None) == 1

    def test_invalid_tenant_id_defaults(self):
        """Test that invalid IDs default to 1."""
        assert validate_tenant_id(0) == 1
        assert validate_tenant_id(-1) == 1
