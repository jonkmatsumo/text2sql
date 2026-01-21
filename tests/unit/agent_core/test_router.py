"""Unit tests for router node and ambiguity detection."""

import importlib.machinery
import importlib.util
import json
import sys
import types
from unittest.mock import AsyncMock, MagicMock, patch

# Mock MCP SDK only if not installed
if importlib.util.find_spec("mcp") is None:

    def create_mock_module(name):
        """Create a mock module for testing."""
        mock = types.ModuleType(name)
        mock.__spec__ = importlib.machinery.ModuleSpec(name, loader=None)
        mock.__path__ = []
        sys.modules[name] = mock
        return mock

    mcp_mock = create_mock_module("mcp")
    mcp_mock.ClientSession = MagicMock()

    mcp_types = create_mock_module("mcp.types")
    mcp_mock.types = mcp_types

    create_mock_module("mcp.client")
    create_mock_module("mcp.client.sse")
    create_mock_module("mcp.client.streamable_http")


import pytest  # noqa: E402
from langchain_core.messages import HumanMessage  # noqa: E402

from agent_core.nodes.router import router_node  # noqa: E402
from agent_core.taxonomy.ambiguity_taxonomy import AMBIGUITY_TAXONOMY  # noqa: E402


@pytest.fixture
def base_state():
    """Create a base agent state for testing."""
    return {
        "messages": [HumanMessage(content="Show me all customers")],
        "schema_context": "Table: customers (id, name, email)",
        "current_sql": None,
        "query_result": None,
        "error": None,
        "retry_count": 0,
        "tenant_id": 1,
    }


@pytest.fixture
def mock_tools():
    """Create a mock list of tools including resolve_ambiguity."""
    mock_resolve_tool = MagicMock()
    mock_resolve_tool.name = "resolve_ambiguity"
    mock_resolve_tool.ainvoke = AsyncMock(return_value=json.dumps([{"status": "CLEAR"}]))
    return [mock_resolve_tool]


def create_mock_span():
    """Create a mock span context manager."""
    mock_span_obj = MagicMock()
    mock_span_obj.set_inputs = MagicMock()
    mock_span_obj.set_outputs = MagicMock()
    mock_span_obj.set_attribute = MagicMock()

    mock_span = MagicMock()
    mock_span.__enter__ = MagicMock(return_value=mock_span_obj)
    mock_span.__exit__ = MagicMock(return_value=None)
    return mock_span


class TestRouterNode:
    """Tests for router_node function."""

    @pytest.mark.asyncio
    async def test_clear_query_routes_to_retrieve(self, base_state, mock_tools):
        """Test that clear query routes to retrieve."""
        response_json = json.dumps(
            {
                "is_ambiguous": False,
                "confidence": 0.95,
            }
        )

        mock_response = MagicMock()
        mock_response.content = response_json

        mock_chain = MagicMock()
        mock_chain.invoke = MagicMock(return_value=mock_response)

        with (
            patch("mlflow.start_span", return_value=create_mock_span()),
            patch("agent_core.nodes.router.ChatPromptTemplate") as mock_prompt,
            patch("agent_core.nodes.router.get_mcp_tools", AsyncMock(return_value=mock_tools)),
            patch("agent_core.llm_client.get_llm"),
        ):
            mock_prompt.from_messages.return_value.__or__ = MagicMock(return_value=mock_chain)

            result = await router_node(base_state)

        assert result.get("ambiguity_type") is None
        assert result.get("clarification_question") is None
        assert result.get("active_query") == "Show me all customers"

    @pytest.mark.asyncio
    async def test_ambiguous_query_sets_clarification(self, base_state, mock_tools):
        """Test that ambiguous query sets clarification question."""
        base_state["messages"] = [HumanMessage(content="Show sales by region")]

        response_json = json.dumps(
            {
                "is_ambiguous": True,
                "ambiguity_type": "UNCLEAR_SCHEMA_REFERENCE",
                "clarification_question": "Do you mean Customer region or Store region?",
                "confidence": 0.6,
            }
        )

        mock_response = MagicMock()
        mock_response.content = response_json

        mock_chain = MagicMock()
        mock_chain.ainvoke = AsyncMock(return_value=mock_response)

        # Mock tool to return AMBIGUOUS
        mock_tools[0].ainvoke = AsyncMock(
            return_value=json.dumps(
                [{"status": "AMBIGUOUS", "ambiguity_type": "UNCLEAR_SCHEMA_REFERENCE"}]
            )
        )

        with (
            patch("mlflow.start_span", return_value=create_mock_span()),
            patch("agent_core.nodes.router.ChatPromptTemplate") as mock_prompt,
            patch("agent_core.nodes.router.get_mcp_tools", AsyncMock(return_value=mock_tools)),
            patch("agent_core.llm_client.get_llm"),
        ):
            mock_prompt.from_messages.return_value.__or__ = MagicMock(return_value=mock_chain)

            result = await router_node(base_state)

        assert result.get("ambiguity_type") == "UNCLEAR_SCHEMA_REFERENCE"
        assert result.get("clarification_question") is not None
        assert result.get("active_query") == "Show sales by region"

    @pytest.mark.asyncio
    async def test_with_existing_clarification_clears_ambiguity(self, base_state):
        """Test that existing clarification clears ambiguity and proceeds."""
        base_state["user_clarification"] = "I meant customer region"

        with (
            patch("mlflow.start_span", return_value=create_mock_span()),
            patch("agent_core.llm_client.get_llm"),
        ):
            result = await router_node(base_state)

        assert result.get("ambiguity_type") is None
        assert result.get("clarification_question") is None
        # Should contain active_query
        # (defaults to messages[-1] if clarification present and no contextualization mock)
        assert result.get("active_query") == "Show me all customers"

    @pytest.mark.asyncio
    async def test_empty_query_returns_empty(self, base_state):
        """Test that empty query returns empty dict."""
        base_state["messages"] = []

        with (
            patch("mlflow.start_span", return_value=create_mock_span()),
            patch("agent_core.llm_client.get_llm"),
        ):
            result = await router_node(base_state)

        assert result == {}

    @pytest.mark.asyncio
    async def test_json_parse_fallback(self, base_state, mock_tools):
        """Test fallback when LLM returns invalid JSON."""
        mock_response = MagicMock()
        mock_response.content = "Not valid JSON at all"

        mock_chain = MagicMock()
        mock_chain.ainvoke = AsyncMock(return_value=mock_response)

        with (
            patch("mlflow.start_span", return_value=create_mock_span()),
            patch("agent_core.nodes.router.ChatPromptTemplate") as mock_prompt,
            patch("agent_core.nodes.router.get_mcp_tools", AsyncMock(return_value=mock_tools)),
            patch("agent_core.llm_client.get_llm"),
        ):
            mock_prompt.from_messages.return_value.__or__ = MagicMock(return_value=mock_chain)

            result = await router_node(base_state)

        # Should default to non-ambiguous on parse failure
        assert result.get("ambiguity_type") is None

    @pytest.mark.asyncio
    async def test_contextualizes_query_with_history(self, base_state, mock_tools):
        """Test that query is contextualized when history exists."""
        base_state["messages"] = [
            HumanMessage(content="First question"),
            HumanMessage(content="Follow up"),
        ]

        # First chain call (contextualize) returns reformulate query
        mock_contextualize_response = MagicMock()
        mock_contextualize_response.content = "Combined Query"

        # Second chain call (ambiguity) returns JSON
        mock_ambiguity_response = MagicMock()
        mock_ambiguity_response.content = json.dumps({"is_ambiguous": False})

        mock_chain = MagicMock()
        # Side effect to return different mocks for different calls?
        # ainvoke is for contextualize, invoke is for ambiguity
        mock_chain.ainvoke = AsyncMock(
            side_effect=[mock_contextualize_response, mock_ambiguity_response]
        )

        with (
            patch("mlflow.start_span", return_value=create_mock_span()),
            patch("agent_core.nodes.router.ChatPromptTemplate") as mock_prompt,
            patch("agent_core.nodes.router.get_mcp_tools", AsyncMock(return_value=mock_tools)),
            patch("agent_core.llm_client.get_llm"),
        ):
            mock_prompt.from_messages.return_value.__or__ = MagicMock(return_value=mock_chain)

            result = await router_node(base_state)

        assert result.get("active_query") == "Combined Query"


class TestAmbiguityTaxonomy:
    """Tests for AMBIGUITY_TAXONOMY structure."""

    def test_taxonomy_has_expected_categories(self):
        """Test that taxonomy has expected ambiguity categories."""
        expected_categories = [
            "UNCLEAR_SCHEMA_REFERENCE",
            "UNCLEAR_VALUE_REFERENCE",
            "MISSING_TEMPORAL_CONSTRAINT",
            "LOGICAL_METRIC_CONFLICT",
            "MISSING_FILTER_CRITERIA",
        ]

        for category in expected_categories:
            assert category in AMBIGUITY_TAXONOMY

    def test_categories_have_required_fields(self):
        """Test that all categories have required fields."""
        for key, category in AMBIGUITY_TAXONOMY.items():
            assert "description" in category
            assert "example" in category
            assert "question_template" in category


class TestHardRefusalLogic:
    """Tests for FAIL-FAST hard refusal when data is missing."""

    @pytest.mark.asyncio
    async def test_hard_refusal_when_missing_data(self, base_state, mock_tools):
        """Test that missing data triggers hard refusal."""
        base_state["messages"] = [HumanMessage(content="Show runtime for Director's Cut movies")]
        base_state[
            "schema_context"
        ] = """## Tables
### film
| Column | Type |
|--------|------|
| film_id | integer |
| runtime | integer |
| title | varchar |
"""  # Note: No 'version' or 'edition' column exists

        response_json = json.dumps(
            {
                "is_ambiguous": False,
                "confidence": 0.9,
                "missing_data": "version/edition information (Director's Cut)",
                "hard_refusal_message": (
                    "I cannot filter by 'Director's Cut' because the database "
                    "does not contain version or edition information. "
                    "I can calculate the average runtime for ALL movies. "
                    "Would you like that instead?"
                ),
            }
        )

        mock_response = MagicMock()
        mock_response.content = response_json

        mock_chain = MagicMock()
        mock_chain.ainvoke = AsyncMock(return_value=mock_response)

        # Mock tool to return MISSING
        mock_tools[0].ainvoke = AsyncMock(
            return_value=json.dumps([{"status": "MISSING", "ambiguity_type": "MISSING_DATA"}])
        )

        with (
            patch("mlflow.start_span", return_value=create_mock_span()),
            patch("agent_core.nodes.router.ChatPromptTemplate") as mock_prompt,
            patch("agent_core.nodes.router.get_mcp_tools", AsyncMock(return_value=mock_tools)),
            patch("agent_core.llm_client.get_llm"),
        ):
            mock_prompt.from_messages.return_value.__or__ = MagicMock(return_value=mock_chain)

            result = await router_node(base_state)

        # Should return MISSING_DATA ambiguity type with refusal message
        assert result.get("ambiguity_type") == "MISSING_DATA"
        assert "Director's Cut" in result.get("clarification_question", "")
        assert "cannot filter" in result.get("clarification_question", "")

    @pytest.mark.asyncio
    async def test_route_question_simple(self, base_state, mock_tools):
        """Test simple question routing."""
        base_state["messages"] = [HumanMessage(content="What is the average runtime?")]
        base_state[
            "schema_context"
        ] = """## Tables
### film
| Column | Type |
|--------|------|
| runtime | integer |
"""

        response_json = json.dumps(
            {
                "is_ambiguous": False,
                "confidence": 0.95,
                "assumptions": ["'runtime' resolved to film.runtime (only match)"],
            }
        )

        mock_response = MagicMock()
        mock_response.content = response_json

        mock_chain = MagicMock()
        mock_chain.ainvoke = AsyncMock(return_value=mock_response)

        with (
            patch("mlflow.start_span", return_value=create_mock_span()),
            patch("agent_core.nodes.router.ChatPromptTemplate") as mock_prompt,
            patch("agent_core.nodes.router.get_mcp_tools", AsyncMock(return_value=mock_tools)),
            patch("agent_core.llm_client.get_llm"),
        ):
            mock_prompt.from_messages.return_value.__or__ = MagicMock(return_value=mock_chain)

            result = await router_node(base_state)

        # Should NOT clarify - proceed silently
        assert result.get("ambiguity_type") is None
        assert result.get("clarification_question") is None

    @pytest.mark.asyncio
    async def test_genuine_ambiguity_multiple_columns(self, base_state, mock_tools):
        """Test that genuine multi-column ambiguity triggers clarification."""
        base_state["messages"] = [HumanMessage(content="Group sales by region")]
        base_state[
            "schema_context"
        ] = """## Tables
### customer
| Column | Type |
|--------|------|
| region | varchar |

### store
| Column | Type |
|--------|------|
| region | varchar |
"""

        response_json = json.dumps(
            {
                "is_ambiguous": True,
                "ambiguity_type": "UNCLEAR_SCHEMA_REFERENCE",
                "clarification_question": "Do you mean region from customers or stores?",
                "confidence": 0.5,
            }
        )

        mock_response = MagicMock()
        mock_response.content = response_json

        mock_chain = MagicMock()
        mock_chain.ainvoke = AsyncMock(return_value=mock_response)

        # Mock tool to return AMBIGUOUS
        mock_tools[0].ainvoke = AsyncMock(
            return_value=json.dumps(
                [{"status": "AMBIGUOUS", "ambiguity_type": "UNCLEAR_SCHEMA_REFERENCE"}]
            )
        )

        with (
            patch("mlflow.start_span", return_value=create_mock_span()),
            patch("agent_core.nodes.router.ChatPromptTemplate") as mock_prompt,
            patch("agent_core.nodes.router.get_mcp_tools", AsyncMock(return_value=mock_tools)),
            patch("agent_core.llm_client.get_llm"),
        ):
            mock_prompt.from_messages.return_value.__or__ = MagicMock(return_value=mock_chain)

            result = await router_node(base_state)

        # Should trigger clarification for genuine ambiguity
        assert result.get("ambiguity_type") == "UNCLEAR_SCHEMA_REFERENCE"
        assert "region" in result.get("clarification_question", "").lower()
