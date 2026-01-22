"""Unit tests for insight synthesis node."""

from unittest.mock import MagicMock, patch

from agent_core.nodes.synthesize import synthesize_insight_node
from agent_core.state import AgentState


class TestSynthesizeInsightNode:
    """Unit tests for synthesize_insight_node function."""

    @patch("agent_core.llm_client.get_llm")
    @patch("agent_core.nodes.synthesize.ChatPromptTemplate")
    def test_synthesize_insight_node_success(self, mock_prompt_class, mock_llm):
        """Test successful insight synthesis with query results."""
        # Create mock prompt template and chain
        mock_prompt = MagicMock()
        mock_chain = MagicMock()
        mock_prompt.from_messages.return_value = mock_prompt
        mock_prompt.__or__ = MagicMock(return_value=mock_chain)
        mock_prompt_class.from_messages.return_value = mock_prompt

        # Create mock response
        mock_response = MagicMock()
        mock_response.content = "There are 1000 films in the database."
        mock_chain.invoke.return_value = mock_response

        # Create test state
        from langchain_core.messages import HumanMessage

        state = AgentState(
            messages=[HumanMessage(content="How many films are there?")],
            schema_context="",
            current_sql="SELECT COUNT(*) FROM film",
            query_result=[{"count": 1000}],
            error=None,
            retry_count=0,
        )

        result = synthesize_insight_node(state)

        # Verify prompt was created
        mock_prompt_class.from_messages.assert_called_once()

        # Verify chain was invoked with correct parameters
        mock_chain.invoke.assert_called_once()
        call_kwargs = mock_chain.invoke.call_args[0][0]
        assert "question" in call_kwargs
        assert "results" in call_kwargs
        assert call_kwargs["question"] == "How many films are there?"
        assert "1000" in call_kwargs["results"]  # JSON string contains the count

        # Verify result contains AIMessage
        assert "messages" in result
        assert len(result["messages"]) == 1
        assert result["messages"][0].content == "There are 1000 films in the database."

    def test_synthesize_insight_node_empty_results(self):
        """Test handling of empty query results."""
        from langchain_core.messages import HumanMessage

        state = AgentState(
            messages=[HumanMessage(content="Show me films")],
            schema_context="",
            current_sql="SELECT * FROM film WHERE 1=0",
            query_result=None,
            error=None,
            retry_count=0,
        )

        result = synthesize_insight_node(state)

        # Verify empty result message
        assert "messages" in result
        assert len(result["messages"]) == 1
        assert "couldn't retrieve any results" in result["messages"][0].content.lower()

    def test_synthesize_insight_node_empty_list_results(self):
        """Test handling of empty list results."""
        from langchain_core.messages import HumanMessage

        state = AgentState(
            messages=[HumanMessage(content="Show me films")],
            schema_context="",
            current_sql="SELECT * FROM film WHERE 1=0",
            query_result=[],
            error=None,
            retry_count=0,
        )

        result = synthesize_insight_node(state)

        # Empty list should still trigger empty result handling
        # (empty list is falsy in Python)
        assert "messages" in result
        assert len(result["messages"]) == 1

    @patch("agent_core.llm_client.get_llm")
    @patch("agent_core.nodes.synthesize.ChatPromptTemplate")
    def test_synthesize_insight_node_large_result_set(self, mock_prompt_class, mock_llm):
        """Test synthesis with large result set."""
        mock_prompt = MagicMock()
        mock_chain = MagicMock()
        mock_prompt.from_messages.return_value = mock_prompt
        mock_prompt.__or__ = MagicMock(return_value=mock_chain)
        mock_prompt_class.from_messages.return_value = mock_prompt

        mock_response = MagicMock()
        mock_response.content = "Found 500 films matching your criteria."
        mock_chain.invoke.return_value = mock_response

        from langchain_core.messages import HumanMessage

        # Create large result set
        large_results = [{"id": i, "title": f"Film {i}"} for i in range(500)]

        state = AgentState(
            messages=[HumanMessage(content="Show me action films")],
            schema_context="",
            current_sql="SELECT * FROM film LIMIT 500",
            query_result=large_results,
            error=None,
            retry_count=0,
        )

        result = synthesize_insight_node(state)

        # Verify LLM was called with large result set
        call_kwargs = mock_chain.invoke.call_args[0][0]
        assert len(call_kwargs["results"]) > 0  # JSON string should be non-empty

        # Verify result contains AIMessage
        assert "messages" in result
        assert result["messages"][0].content == "Found 500 films matching your criteria."

    @patch("agent_core.llm_client.get_llm")
    @patch("agent_core.nodes.synthesize.ChatPromptTemplate")
    def test_synthesize_insight_node_json_serialization(self, mock_prompt_class, mock_llm):
        """Test JSON serialization handles complex types (dates, decimals)."""
        from datetime import date
        from decimal import Decimal

        mock_prompt = MagicMock()
        mock_chain = MagicMock()
        mock_prompt.from_messages.return_value = mock_prompt
        mock_prompt.__or__ = MagicMock(return_value=mock_chain)
        mock_prompt_class.from_messages.return_value = mock_prompt

        mock_response = MagicMock()
        mock_response.content = "Payment processed on 2024-01-15 for $99.99"
        mock_chain.invoke.return_value = mock_response

        from langchain_core.messages import HumanMessage

        # Create result with date and decimal
        state = AgentState(
            messages=[HumanMessage(content="Show payment details")],
            schema_context="",
            current_sql="SELECT * FROM payment LIMIT 1",
            query_result=[
                {
                    "payment_id": 1,
                    "amount": Decimal("99.99"),
                    "payment_date": date(2024, 1, 15),
                }
            ],
            error=None,
            retry_count=0,
        )

        result = synthesize_insight_node(state)

        # Verify JSON serialization worked (default=str handles dates/decimals)
        call_kwargs = mock_chain.invoke.call_args[0][0]
        assert "99.99" in call_kwargs["results"]
        assert "2024-01-15" in call_kwargs["results"]

        # Verify result contains AIMessage
        assert "messages" in result
        assert len(result["messages"]) == 1

    @patch("agent_core.llm_client.get_llm")
    @patch("agent_core.nodes.synthesize.ChatPromptTemplate")
    def test_synthesize_insight_node_multiple_messages(self, mock_prompt_class, mock_llm):
        """Test that original question is extracted from first message."""
        mock_prompt = MagicMock()
        mock_chain = MagicMock()
        mock_prompt.from_messages.return_value = mock_prompt
        mock_prompt.__or__ = MagicMock(return_value=mock_chain)
        mock_prompt_class.from_messages.return_value = mock_prompt

        mock_response = MagicMock()
        mock_response.content = "Result summary"
        mock_chain.invoke.return_value = mock_response

        from langchain_core.messages import AIMessage, HumanMessage

        # Create state with multiple messages
        state = AgentState(
            messages=[
                HumanMessage(content="How many customers?"),
                AIMessage(content="I'll check the database."),
            ],
            schema_context="",
            current_sql="SELECT COUNT(*) FROM customer",
            query_result=[{"count": 599}],
            error=None,
            retry_count=0,
        )

        result = synthesize_insight_node(state)

        # Verify first message content was used
        call_kwargs = mock_chain.invoke.call_args[0][0]
        assert call_kwargs["question"] == "How many customers?"

        assert "messages" in result

    @patch("agent_core.llm_client.get_llm")
    @patch("agent_core.nodes.synthesize.ChatPromptTemplate")
    def test_synthesize_insight_node_returns_aimessage(self, mock_prompt_class, mock_llm):
        """Test that result contains AIMessage objects, not dicts."""
        mock_prompt = MagicMock()
        mock_chain = MagicMock()
        mock_prompt.from_messages.return_value = mock_prompt
        mock_prompt.__or__ = MagicMock(return_value=mock_chain)
        mock_prompt_class.from_messages.return_value = mock_prompt

        mock_response = MagicMock()
        mock_response.content = "Test response"
        mock_chain.invoke.return_value = mock_response

        from langchain_core.messages import AIMessage, HumanMessage

        state = AgentState(
            messages=[HumanMessage(content="Test question")],
            schema_context="",
            current_sql="SELECT 1",
            query_result=[{"value": 1}],
            error=None,
            retry_count=0,
        )

        result = synthesize_insight_node(state)

        # Verify result contains AIMessage object
        assert "messages" in result
        assert len(result["messages"]) == 1
        assert isinstance(result["messages"][0], AIMessage)
        assert result["messages"][0].content == "Test response"
