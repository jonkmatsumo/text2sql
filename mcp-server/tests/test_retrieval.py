"""Tests for dynamic few-shot learning retrieval."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from mcp_server.retrieval import get_relevant_examples


class TestGetRelevantExamples:
    """Unit tests for get_relevant_examples function."""

    @pytest.mark.asyncio
    async def test_retrieval_returns_examples(self):
        """Test that retrieval returns formatted string with examples."""
        mock_conn = AsyncMock()
        mock_rows = [
            {
                "question": "What is the total revenue?",
                "sql_query": "SELECT SUM(amount) FROM payment;",
                "summary": "Calculates total revenue from all payments",
                "similarity": 0.95,
            },
            {
                "question": "Show me monthly revenue",
                "sql_query": (
                    "SELECT DATE_TRUNC('month', payment_date), SUM(amount) "
                    "FROM payment GROUP BY 1;"
                ),
                "summary": "Groups payments by month and sums amounts",
                "similarity": 0.92,
            },
        ]
        mock_conn.fetch = AsyncMock(return_value=mock_rows)

        # Setup async context manager mock
        mock_get_cm = AsyncMock()
        mock_get_cm.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_get_cm.__aexit__ = AsyncMock(return_value=False)
        mock_get = MagicMock(return_value=mock_get_cm)

        mock_embedding = [0.1] * 384
        mock_pg_vector = "[0.1,0.1,...]"

        with patch("mcp_server.retrieval.Database.get_connection", mock_get):
            with patch("mcp_server.retrieval.RagEngine.embed_text", return_value=mock_embedding):
                with patch(
                    "mcp_server.retrieval.format_vector_for_postgres", return_value=mock_pg_vector
                ):
                    examples = await get_relevant_examples("Show me total revenue", limit=3)

                    # Should return a JSON string
                    assert isinstance(examples, str)
                    # Verify it contains the JSON structure we expect
                    assert '"question":"What is the total revenue?"' in examples
                    assert '"sql":"SELECT SUM(amount) FROM payment;"' in examples

                    # Verify connection was acquired
                    mock_get.assert_called_once()

    @pytest.mark.asyncio
    async def test_retrieval_limit(self):
        """Test that retrieval respects limit parameter."""
        mock_conn = AsyncMock()
        # Return 5 rows but limit should be 1
        mock_rows = [
            {
                "question": f"Question {i}",
                "sql_query": f"SELECT * FROM table{i};",
                "summary": f"Summary {i}",
                "similarity": 0.9 - i * 0.1,
            }
            for i in range(5)
        ]
        mock_conn.fetch = AsyncMock(return_value=mock_rows)

        # Setup async context manager mock
        mock_get_cm = AsyncMock()
        mock_get_cm.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_get_cm.__aexit__ = AsyncMock(return_value=False)
        mock_get = MagicMock(return_value=mock_get_cm)

        mock_embedding = [0.1] * 384
        mock_pg_vector = "[0.1,0.1,...]"

        with patch("mcp_server.retrieval.Database.get_connection", mock_get):
            with patch("mcp_server.retrieval.RagEngine.embed_text", return_value=mock_embedding):
                with patch(
                    "mcp_server.retrieval.format_vector_for_postgres", return_value=mock_pg_vector
                ):
                    await get_relevant_examples("test query", limit=1)

                    # Verify limit was passed to query
                    call_args = mock_conn.fetch.call_args[0]
                    assert call_args[2] == 1  # limit parameter

                    # Verify connection was acquired
                    mock_get.assert_called_once()

    @pytest.mark.asyncio
    async def test_retrieval_empty_result(self):
        """Test behavior when no examples exist."""
        mock_conn = AsyncMock()
        mock_conn.fetch = AsyncMock(return_value=[])

        # Setup async context manager mock
        mock_get_cm = AsyncMock()
        mock_get_cm.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_get_cm.__aexit__ = AsyncMock(return_value=False)
        mock_get = MagicMock(return_value=mock_get_cm)

        mock_embedding = [0.1] * 384
        mock_pg_vector = "[0.1,0.1,...]"

        with patch("mcp_server.retrieval.Database.get_connection", mock_get):
            with patch("mcp_server.retrieval.RagEngine.embed_text", return_value=mock_embedding):
                with patch(
                    "mcp_server.retrieval.format_vector_for_postgres", return_value=mock_pg_vector
                ):
                    examples = await get_relevant_examples("test query", limit=3)

                    # Should return empty string when no examples found
                    assert examples == ""
                    mock_get.assert_called_once()

    @pytest.mark.asyncio
    async def test_retrieval_format(self):
        """Verify output format contains required sections."""
        mock_conn = AsyncMock()
        mock_rows = [
            {
                "question": "Test question",
                "sql_query": "SELECT * FROM test;",
                "summary": "Test summary",
                "similarity": 0.95,
            },
        ]
        mock_conn.fetch = AsyncMock(return_value=mock_rows)

        # Setup async context manager mock
        mock_get_cm = AsyncMock()
        mock_get_cm.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_get_cm.__aexit__ = AsyncMock(return_value=False)
        mock_get = MagicMock(return_value=mock_get_cm)

        mock_embedding = [0.1] * 384
        mock_pg_vector = "[0.1,0.1,...]"

        with patch("mcp_server.retrieval.Database.get_connection", mock_get):
            with patch("mcp_server.retrieval.RagEngine.embed_text", return_value=mock_embedding):
                with patch(
                    "mcp_server.retrieval.format_vector_for_postgres", return_value=mock_pg_vector
                ):
                    examples = await get_relevant_examples("test query", limit=3)

                    # Verify format contains all required sections in JSON
                    assert '"question":"Test question"' in examples
                    assert '"sql":"SELECT * FROM test;"' in examples

    @pytest.mark.asyncio
    async def test_retrieval_uses_context_manager(self):
        """Verify database connection uses context manager pattern."""
        mock_conn = AsyncMock()
        mock_conn.fetch = AsyncMock(return_value=[])

        # Setup async context manager mock
        mock_get_cm = AsyncMock()
        mock_get_cm.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_get_cm.__aexit__ = AsyncMock(return_value=False)
        mock_get = MagicMock(return_value=mock_get_cm)

        mock_embedding = [0.1] * 384
        mock_pg_vector = "[0.1,0.1,...]"

        with patch("mcp_server.retrieval.Database.get_connection", mock_get):
            with patch("mcp_server.retrieval.RagEngine.embed_text", return_value=mock_embedding):
                with patch(
                    "mcp_server.retrieval.format_vector_for_postgres", return_value=mock_pg_vector
                ):
                    await get_relevant_examples("test query", limit=3)

                    # Verify context manager was used
                    mock_get.assert_called_once()
                    mock_get_cm.__aenter__.assert_called_once()
                    mock_get_cm.__aexit__.assert_called_once()

    @pytest.mark.asyncio
    async def test_retrieval_without_summary(self):
        """Test that retrieval handles examples without summaries gracefully."""
        mock_conn = AsyncMock()
        mock_rows = [
            {
                "question": "Test question",
                "sql_query": "SELECT * FROM test;",
                "summary": None,  # No summary
                "similarity": 0.95,
            },
        ]
        mock_conn.fetch = AsyncMock(return_value=mock_rows)

        # Setup async context manager mock
        mock_get_cm = AsyncMock()
        mock_get_cm.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_get_cm.__aexit__ = AsyncMock(return_value=False)
        mock_get = MagicMock(return_value=mock_get_cm)

        mock_embedding = [0.1] * 384
        mock_pg_vector = "[0.1,0.1,...]"

        with patch("mcp_server.retrieval.Database.get_connection", mock_get):
            with patch("mcp_server.retrieval.RagEngine.embed_text", return_value=mock_embedding):
                with patch(
                    "mcp_server.retrieval.format_vector_for_postgres", return_value=mock_pg_vector
                ):
                    examples = await get_relevant_examples("test query", limit=3)

                    # Should still format correctly without summary
                    assert '"question":"Test question"' in examples
                    assert '"sql":"SELECT * FROM test;"' in examples
