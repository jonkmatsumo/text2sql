"""Unit tests for SQL correction node."""

from unittest.mock import MagicMock, patch

import pytest

from agent.nodes.correct import correct_sql_node
from agent.state import AgentState
from common.utils.hashing import canonical_json_hash


class TestCorrectSqlNode:
    """Unit tests for correct_sql_node function."""

    @pytest.fixture(autouse=True)
    def disable_similarity(self):
        """Disable similarity enforcement for these tests."""
        with patch.dict("os.environ", {"AGENT_CORRECTION_SIMILARITY_ENFORCE": "False"}):
            yield

    @patch("agent.llm_client.get_llm")
    @patch("agent.nodes.correct.ChatPromptTemplate")
    def test_correct_sql_node_success(self, mock_prompt_class, mock_llm):
        """Test successful SQL correction."""
        # Create mock prompt template and chain
        mock_prompt = MagicMock()
        mock_chain = MagicMock()
        mock_prompt.from_messages.return_value = mock_prompt
        mock_prompt.__or__ = MagicMock(return_value=mock_chain)
        mock_prompt_class.from_messages.return_value = mock_prompt

        # Create mock response
        mock_response = MagicMock()
        mock_response.content = "SELECT * FROM film"
        mock_chain.invoke.return_value = mock_response

        state = AgentState(
            messages=[],
            schema_context="Table: film. Columns: film_id, title",
            current_sql="SELECT * FROM films",  # Wrong table name
            query_result=None,
            error='relation "films" does not exist',
            retry_count=0,
        )

        result = correct_sql_node(state)

        # Verify prompt was created
        mock_prompt_class.from_messages.assert_called_once()

        # Verify chain was invoked with correct parameters
        mock_chain.invoke.assert_called_once()
        call_kwargs = mock_chain.invoke.call_args[0][0]
        assert "schema_context" in call_kwargs
        assert "bad_query" in call_kwargs
        assert "error_msg" in call_kwargs
        assert call_kwargs["bad_query"] == "SELECT * FROM films"
        assert call_kwargs["error_msg"] == 'relation "films" does not exist'

        # Verify corrected SQL
        assert result["current_sql"] == "SELECT * FROM film"
        # Verify retry_count incremented
        assert result["retry_count"] == 1
        # Verify error reset
        assert result["error"] is None

    @patch("agent.llm_client.get_llm")
    @patch("agent.nodes.correct.ChatPromptTemplate")
    def test_correct_sql_node_prompt_budget_exceeded(
        self, mock_prompt_class, mock_llm, monkeypatch
    ):
        """Prompt-byte budget should stop correction before invoking the LLM."""
        monkeypatch.setenv("AGENT_MAX_PROMPT_BYTES_PER_RUN", "200")
        mock_prompt = MagicMock()
        mock_chain = MagicMock()
        mock_prompt.from_messages.return_value = mock_prompt
        mock_prompt.__or__ = MagicMock(return_value=mock_chain)
        mock_prompt_class.from_messages.return_value = mock_prompt

        state = AgentState(
            messages=[],
            schema_context=("x" * 5000),
            current_sql="SELECT * FROM films",
            query_result=None,
            error='relation "films" does not exist',
            retry_count=0,
            llm_prompt_bytes_used=0,
            llm_budget_exceeded=False,
        )

        result = correct_sql_node(state)

        mock_chain.invoke.assert_not_called()
        mock_llm.assert_not_called()
        assert result["error_category"] == "budget_exhausted"
        assert result["llm_budget_exceeded"] is True

    @patch("agent.llm_client.get_llm")
    @patch("agent.nodes.correct.ChatPromptTemplate")
    def test_correct_sql_node_retry_count_increment(self, mock_prompt_class, mock_llm):
        """Test that retry_count is incremented."""
        mock_prompt = MagicMock()
        mock_chain = MagicMock()
        mock_prompt.from_messages.return_value = mock_prompt
        mock_prompt.__or__ = MagicMock(return_value=mock_chain)
        mock_prompt_class.from_messages.return_value = mock_prompt

        mock_response = MagicMock()
        mock_response.content = "SELECT film_id FROM film"
        mock_chain.invoke.return_value = mock_response

        state = AgentState(
            messages=[],
            schema_context="Table: film",
            current_sql="SELECT id FROM film",
            query_result=None,
            error="column 'id' does not exist",
            retry_count=2,
        )

        result = correct_sql_node(state)

        assert result["retry_count"] == 3

    @patch("agent.llm_client.get_llm")
    @patch("agent.nodes.correct.ChatPromptTemplate")
    def test_correct_sql_node_error_reset(self, mock_prompt_class, mock_llm):
        """Test that error is reset to None."""
        mock_prompt = MagicMock()
        mock_chain = MagicMock()
        mock_prompt.from_messages.return_value = mock_prompt
        mock_prompt.__or__ = MagicMock(return_value=mock_chain)
        mock_prompt_class.from_messages.return_value = mock_prompt

        mock_response = MagicMock()
        mock_response.content = "SELECT * FROM film"
        mock_chain.invoke.return_value = mock_response

        state = AgentState(
            messages=[],
            schema_context="",
            current_sql="SELECT * FROM films",
            query_result=None,
            error="Some error",
            retry_count=0,
        )

        result = correct_sql_node(state)

        assert result["error"] is None

    @patch("agent.llm_client.get_llm")
    @patch("agent.nodes.correct.ChatPromptTemplate")
    def test_correct_sql_node_markdown_removal(self, mock_prompt_class, mock_llm):
        """Test markdown code block removal from corrected SQL."""
        mock_prompt = MagicMock()
        mock_chain = MagicMock()
        mock_prompt.from_messages.return_value = mock_prompt
        mock_prompt.__or__ = MagicMock(return_value=mock_chain)
        mock_prompt_class.from_messages.return_value = mock_prompt

        mock_response = MagicMock()
        mock_response.content = "```sql\nSELECT * FROM film\n```"
        mock_chain.invoke.return_value = mock_response

        state = AgentState(
            messages=[],
            schema_context="",
            current_sql="SELECT * FROM films",
            query_result=None,
            error="Error",
            retry_count=0,
        )

        result = correct_sql_node(state)

        assert result["current_sql"] == "SELECT * FROM film"

    @patch("agent.llm_client.get_llm")
    @patch("agent.nodes.correct.ChatPromptTemplate")
    def test_correct_sql_node_empty_schema_context(self, mock_prompt_class, mock_llm):
        """Test correction with empty schema_context."""
        mock_prompt = MagicMock()
        mock_chain = MagicMock()
        mock_prompt.from_messages.return_value = mock_prompt
        mock_prompt.__or__ = MagicMock(return_value=mock_chain)
        mock_prompt_class.from_messages.return_value = mock_prompt

        mock_response = MagicMock()
        mock_response.content = "SELECT 1"
        mock_chain.invoke.return_value = mock_response

        state = AgentState(
            messages=[],
            schema_context="",
            current_sql="SELECT * FROM wrong",
            query_result=None,
            error="Error",
            retry_count=0,
        )

        result = correct_sql_node(state)

        # Verify empty schema_context was passed
        call_kwargs = mock_chain.invoke.call_args[0][0]
        assert call_kwargs["schema_context"] == ""

        assert result["current_sql"] == "SELECT 1"

    @patch("agent.llm_client.get_llm")
    @patch("agent.nodes.correct.ChatPromptTemplate")
    def test_correct_sql_node_missing_retry_count(self, mock_prompt_class, mock_llm):
        """Test that retry_count defaults to 0 if missing."""
        mock_prompt = MagicMock()
        mock_chain = MagicMock()
        mock_prompt.from_messages.return_value = mock_prompt
        mock_prompt.__or__ = MagicMock(return_value=mock_chain)
        mock_prompt_class.from_messages.return_value = mock_prompt

        mock_response = MagicMock()
        mock_response.content = "SELECT * FROM film"
        mock_chain.invoke.return_value = mock_response

        # Create state without retry_count (should default to 0)
        # Use dict.get() to test the default value path
        state = {
            "messages": [],
            "schema_context": "",
            "current_sql": "SELECT * FROM films",
            "query_result": None,
            "error": "Error",
            # retry_count is missing - will use .get() default
        }

        result = correct_sql_node(state)

        # Should increment from 0 to 1
        assert result["retry_count"] == 1

    @patch("agent.llm_client.get_llm")
    @patch("agent.nodes.correct.ChatPromptTemplate")
    def test_correct_sql_node_column_name_error(self, mock_prompt_class, mock_llm):
        """Test correction of column name errors."""
        mock_prompt = MagicMock()
        mock_chain = MagicMock()
        mock_prompt.from_messages.return_value = mock_prompt
        mock_prompt.__or__ = MagicMock(return_value=mock_chain)
        mock_prompt_class.from_messages.return_value = mock_prompt

        mock_response = MagicMock()
        mock_response.content = "SELECT film_id, title FROM film"
        mock_chain.invoke.return_value = mock_response

        state = AgentState(
            messages=[],
            schema_context="Table: film. Columns: film_id, title",
            current_sql="SELECT id, name FROM film",
            query_result=None,
            error="column 'id' does not exist",
            retry_count=0,
        )

        result = correct_sql_node(state)

        assert result["current_sql"] == "SELECT film_id, title FROM film"
        assert result["retry_count"] == 1
        assert result["error"] is None

    @patch("agent.llm_client.get_llm")
    @patch("agent.nodes.correct.ChatPromptTemplate")
    def test_correct_sql_node_syntax_error(self, mock_prompt_class, mock_llm):
        """Test correction of syntax errors."""
        mock_prompt = MagicMock()
        mock_chain = MagicMock()
        mock_prompt.from_messages.return_value = mock_prompt
        mock_prompt.__or__ = MagicMock(return_value=mock_chain)
        mock_prompt_class.from_messages.return_value = mock_prompt

        mock_response = MagicMock()
        mock_response.content = "SELECT * FROM film WHERE rating = 'PG'"
        mock_chain.invoke.return_value = mock_response

        state = AgentState(
            messages=[],
            schema_context="Table: film",
            current_sql="SELECT * FROM film WHERE rating = PG",
            query_result=None,
            error="syntax error at or near 'PG'",
            retry_count=0,
        )

        result = correct_sql_node(state)

        assert result["current_sql"] == "SELECT * FROM film WHERE rating = 'PG'"
        assert result["retry_count"] == 1

    @patch("agent.llm_client.get_llm")
    @patch("agent.nodes.correct.ChatPromptTemplate")
    def test_correct_sql_node_complex_markdown(self, mock_prompt_class, mock_llm):
        """Test SQL extraction from complex markdown format."""
        mock_prompt = MagicMock()
        mock_chain = MagicMock()
        mock_prompt.from_messages.return_value = mock_prompt
        mock_prompt.__or__ = MagicMock(return_value=mock_chain)
        mock_prompt_class.from_messages.return_value = mock_prompt

        mock_response = MagicMock()
        mock_response.content = "```sql\n  SELECT COUNT(*)\n  FROM film\n  WHERE rating = 'PG'\n```"
        mock_chain.invoke.return_value = mock_response

        state = AgentState(
            messages=[],
            schema_context="",
            current_sql="SELECT COUNT(*) FROM films",
            query_result=None,
            error="Error",
            retry_count=0,
        )

        result = correct_sql_node(state)

        expected_sql = "SELECT COUNT(*)\n  FROM film\n  WHERE rating = 'PG'"
        assert result["current_sql"] == expected_sql

    def test_correct_sql_node_tracks_budget_exhaustion_in_correction_attempts(self):
        """Budget-exhausted exits should still produce correction attempt telemetry state."""
        state = AgentState(
            messages=[],
            schema_context="",
            current_sql="SELECT * FROM films",
            query_result=None,
            error="syntax error",
            retry_count=0,
            token_budget={"max_tokens": 10, "consumed_tokens": 10},
        )

        result = correct_sql_node(state)

        assert result["error_category"] == "budget_exhausted"
        assert result["correction_attempts"][-1]["outcome"] == "budget_exhausted"

    def test_correct_sql_node_tracks_repeated_error_stop(self):
        """Repeated-signature exits should preserve correction attempts and outcome."""
        error_msg = "syntax error near from"
        signature = canonical_json_hash(
            {"category": "SYNTAX_ERROR", "message": error_msg.strip().lower()}
        )
        state = AgentState(
            messages=[],
            schema_context="",
            current_sql="SELECT * FROM films",
            query_result=None,
            error=error_msg,
            error_category="SYNTAX_ERROR",
            retry_count=0,
            error_signatures=[signature],
        )

        result = correct_sql_node(state)

        assert result["error_category"] == "repeated_error"
        assert result["correction_attempts"][-1]["outcome"] == "repeated_error"

    def test_correction_attempts_memory_cap_sets_truncation_metadata(self, monkeypatch):
        """Correction history should stay bounded and expose truncation metadata."""
        monkeypatch.setenv("AGENT_RETRY_SUMMARY_MAX_EVENTS", "1")
        state = AgentState(
            messages=[],
            schema_context="",
            current_sql="SELECT * FROM films",
            query_result=None,
            error="syntax error",
            retry_count=0,
            token_budget={"max_tokens": 10, "consumed_tokens": 10},
            correction_attempts=[{"attempt": 0, "outcome": "seed"}],
        )

        result = correct_sql_node(state)

        assert len(result["correction_attempts"]) == 1
        assert result["correction_attempts_truncated"] is True
        assert result["correction_attempts_dropped"] >= 1
