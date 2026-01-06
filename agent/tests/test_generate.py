"""Unit tests for SQL generation node."""

from unittest.mock import MagicMock, patch

from agent_core.nodes.generate import generate_sql_node
from agent_core.state import AgentState


class TestGenerateSqlNode:
    """Unit tests for generate_sql_node function."""

    @patch("agent_core.nodes.generate.llm")
    @patch("agent_core.nodes.generate.ChatPromptTemplate")
    def test_generate_sql_node_success(self, mock_prompt_class, mock_llm):
        """Test successful SQL generation."""
        # Create mock prompt template and chain
        mock_prompt = MagicMock()
        mock_chain = MagicMock()
        mock_prompt.from_messages.return_value = mock_prompt
        mock_prompt.__or__ = MagicMock(return_value=mock_chain)
        mock_prompt_class.from_messages.return_value = mock_prompt

        # Create mock response
        mock_response = MagicMock()
        mock_response.content = "SELECT COUNT(*) FROM film"
        mock_chain.invoke.return_value = mock_response

        # Create test state
        from langchain_core.messages import HumanMessage

        state = AgentState(
            messages=[HumanMessage(content="How many films are there?")],
            schema_context="Table: film. Columns: film_id, title",
            current_sql=None,
            query_result=None,
            error=None,
            retry_count=0,
        )

        result = generate_sql_node(state)

        # Verify prompt was created
        mock_prompt_class.from_messages.assert_called_once()

        # Verify chain was invoked with correct parameters
        mock_chain.invoke.assert_called_once()
        call_kwargs = mock_chain.invoke.call_args[0][0]
        assert "schema_context" in call_kwargs
        assert "question" in call_kwargs
        assert call_kwargs["question"] == "How many films are there?"
        assert call_kwargs["schema_context"] == "Table: film. Columns: film_id, title"

        # Verify SQL was extracted
        assert result["current_sql"] == "SELECT COUNT(*) FROM film"

    @patch("agent_core.nodes.generate.llm")
    @patch("agent_core.nodes.generate.ChatPromptTemplate")
    def test_generate_sql_node_markdown_sql_block(self, mock_prompt_class, mock_llm):
        """Test SQL extraction from markdown code block with ```sql."""
        mock_prompt = MagicMock()
        mock_chain = MagicMock()
        mock_prompt.from_messages.return_value = mock_prompt
        mock_prompt.__or__ = MagicMock(return_value=mock_chain)
        mock_prompt_class.from_messages.return_value = mock_prompt

        mock_response = MagicMock()
        mock_response.content = "```sql\nSELECT * FROM film\n```"
        mock_chain.invoke.return_value = mock_response

        from langchain_core.messages import HumanMessage

        state = AgentState(
            messages=[HumanMessage(content="Show all films")],
            schema_context="Table: film",
            current_sql=None,
            query_result=None,
            error=None,
            retry_count=0,
        )

        result = generate_sql_node(state)

        assert result["current_sql"] == "SELECT * FROM film"

    @patch("agent_core.nodes.generate.llm")
    @patch("agent_core.nodes.generate.ChatPromptTemplate")
    def test_generate_sql_node_markdown_block(self, mock_prompt_class, mock_llm):
        """Test SQL extraction from markdown code block with ```."""
        mock_prompt = MagicMock()
        mock_chain = MagicMock()
        mock_prompt.from_messages.return_value = mock_prompt
        mock_prompt.__or__ = MagicMock(return_value=mock_chain)
        mock_prompt_class.from_messages.return_value = mock_prompt

        mock_response = MagicMock()
        mock_response.content = "```\nSELECT * FROM film\n```"
        mock_chain.invoke.return_value = mock_response

        from langchain_core.messages import HumanMessage

        state = AgentState(
            messages=[HumanMessage(content="Show all films")],
            schema_context="Table: film",
            current_sql=None,
            query_result=None,
            error=None,
            retry_count=0,
        )

        result = generate_sql_node(state)

        assert result["current_sql"] == "SELECT * FROM film"

    @patch("agent_core.nodes.generate.llm")
    @patch("agent_core.nodes.generate.ChatPromptTemplate")
    def test_generate_sql_node_no_markdown(self, mock_prompt_class, mock_llm):
        """Test SQL extraction when no markdown is present."""
        mock_prompt = MagicMock()
        mock_chain = MagicMock()
        mock_prompt.from_messages.return_value = mock_prompt
        mock_prompt.__or__ = MagicMock(return_value=mock_chain)
        mock_prompt_class.from_messages.return_value = mock_prompt

        mock_response = MagicMock()
        mock_response.content = "SELECT * FROM film"
        mock_chain.invoke.return_value = mock_response

        from langchain_core.messages import HumanMessage

        state = AgentState(
            messages=[HumanMessage(content="Show all films")],
            schema_context="Table: film",
            current_sql=None,
            query_result=None,
            error=None,
            retry_count=0,
        )

        result = generate_sql_node(state)

        assert result["current_sql"] == "SELECT * FROM film"

    @patch("agent_core.nodes.generate.llm")
    @patch("agent_core.nodes.generate.ChatPromptTemplate")
    def test_generate_sql_node_empty_schema_context(self, mock_prompt_class, mock_llm):
        """Test SQL generation with empty schema_context."""
        mock_prompt = MagicMock()
        mock_chain = MagicMock()
        mock_prompt.from_messages.return_value = mock_prompt
        mock_prompt.__or__ = MagicMock(return_value=mock_chain)
        mock_prompt_class.from_messages.return_value = mock_prompt

        mock_response = MagicMock()
        mock_response.content = "SELECT 1"
        mock_chain.invoke.return_value = mock_response

        from langchain_core.messages import HumanMessage

        state = AgentState(
            messages=[HumanMessage(content="Test query")],
            schema_context="",
            current_sql=None,
            query_result=None,
            error=None,
            retry_count=0,
        )

        result = generate_sql_node(state)

        # Verify empty schema_context was passed
        call_kwargs = mock_chain.invoke.call_args[0][0]
        assert call_kwargs["schema_context"] == ""

        assert result["current_sql"] == "SELECT 1"

    @patch("agent_core.nodes.generate.llm")
    @patch("agent_core.nodes.generate.ChatPromptTemplate")
    def test_generate_sql_node_multiple_messages(self, mock_prompt_class, mock_llm):
        """Test that question is extracted from last message."""
        mock_prompt = MagicMock()
        mock_chain = MagicMock()
        mock_prompt.from_messages.return_value = mock_prompt
        mock_prompt.__or__ = MagicMock(return_value=mock_chain)
        mock_prompt_class.from_messages.return_value = mock_prompt

        mock_response = MagicMock()
        mock_response.content = "SELECT * FROM customer"
        mock_chain.invoke.return_value = mock_response

        from langchain_core.messages import AIMessage, HumanMessage

        state = AgentState(
            messages=[
                HumanMessage(content="First question"),
                AIMessage(content="Response"),
                HumanMessage(content="Second question"),
            ],
            schema_context="Table: customer",
            current_sql=None,
            query_result=None,
            error=None,
            retry_count=0,
        )

        result = generate_sql_node(state)

        # Verify last message content was used
        call_kwargs = mock_chain.invoke.call_args[0][0]
        assert call_kwargs["question"] == "Second question"

        assert result["current_sql"] == "SELECT * FROM customer"

    @patch("agent_core.nodes.generate.llm")
    @patch("agent_core.nodes.generate.ChatPromptTemplate")
    def test_generate_sql_node_whitespace_handling(self, mock_prompt_class, mock_llm):
        """Test that whitespace is properly stripped."""
        mock_prompt = MagicMock()
        mock_chain = MagicMock()
        mock_prompt.from_messages.return_value = mock_prompt
        mock_prompt.__or__ = MagicMock(return_value=mock_chain)
        mock_prompt_class.from_messages.return_value = mock_prompt

        mock_response = MagicMock()
        mock_response.content = "  SELECT * FROM film  "
        mock_chain.invoke.return_value = mock_response

        from langchain_core.messages import HumanMessage

        state = AgentState(
            messages=[HumanMessage(content="Test")],
            schema_context="Table: film",
            current_sql=None,
            query_result=None,
            error=None,
            retry_count=0,
        )

        result = generate_sql_node(state)

        assert result["current_sql"] == "SELECT * FROM film"

    @patch("agent_core.nodes.generate.llm")
    @patch("agent_core.nodes.generate.ChatPromptTemplate")
    def test_generate_sql_node_complex_markdown(self, mock_prompt_class, mock_llm):
        """Test SQL extraction from complex markdown format."""
        mock_prompt = MagicMock()
        mock_chain = MagicMock()
        mock_prompt.from_messages.return_value = mock_prompt
        mock_prompt.__or__ = MagicMock(return_value=mock_chain)
        mock_prompt_class.from_messages.return_value = mock_prompt

        mock_response = MagicMock()
        mock_response.content = (
            "```sql\n  SELECT COUNT(*) as count\n  FROM film\n  WHERE rating = 'PG'\n```"
        )
        mock_chain.invoke.return_value = mock_response

        from langchain_core.messages import HumanMessage

        state = AgentState(
            messages=[HumanMessage(content="How many PG films?")],
            schema_context="Table: film. Columns: film_id, rating",
            current_sql=None,
            query_result=None,
            error=None,
            retry_count=0,
        )

        result = generate_sql_node(state)

        expected_sql = "SELECT COUNT(*) as count\n  FROM film\n  WHERE rating = 'PG'"
        assert result["current_sql"] == expected_sql
