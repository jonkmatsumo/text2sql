"""Unit tests for seeding module."""

from unittest.mock import AsyncMock, patch

import pytest


class TestGenerateMissingEmbeddings:
    """Tests for generate_missing_embeddings function."""

    @pytest.mark.asyncio
    @patch("mcp_server.seeding.examples.Database")
    @patch("mcp_server.seeding.examples.RagEngine")
    @patch("mcp_server.seeding.examples.format_vector_for_postgres")
    async def test_generates_embeddings_for_null_rows(self, mock_format, mock_rag, mock_db):
        """Test that embeddings are generated for rows with NULL embedding."""
        from mcp_server.seeding.examples import generate_missing_embeddings

        # Setup mock data
        mock_rows = [
            {"id": 1, "question": "Q1", "summary": "Q1"},
            {"id": 2, "question": "Q2", "summary": "Q2"},
        ]

        # Mock database connection
        mock_conn = AsyncMock()
        mock_conn.fetch = AsyncMock(return_value=mock_rows)
        mock_conn.execute = AsyncMock()

        mock_context = AsyncMock()
        mock_context.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_context.__aexit__ = AsyncMock(return_value=None)
        mock_db.get_connection.return_value = mock_context

        # Mock embedding generation
        mock_rag.embed_text.return_value = [0.1, 0.2, 0.3]
        mock_format.return_value = "[0.1,0.2,0.3]"

        result = await generate_missing_embeddings()

        assert result == 2
        assert mock_rag.embed_text.call_count == 2
        assert mock_conn.execute.call_count == 2

    @pytest.mark.asyncio
    @patch("mcp_server.seeding.examples.Database")
    async def test_returns_zero_when_no_missing_embeddings(self, mock_db):
        """Test that 0 is returned when all rows have embeddings."""
        from mcp_server.seeding.examples import generate_missing_embeddings

        # Mock empty result
        mock_conn = AsyncMock()
        mock_conn.fetch = AsyncMock(return_value=[])

        mock_context = AsyncMock()
        mock_context.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_context.__aexit__ = AsyncMock(return_value=None)
        mock_db.get_connection.return_value = mock_context

        result = await generate_missing_embeddings()

        assert result == 0

    @pytest.mark.asyncio
    @patch("mcp_server.seeding.examples.Database")
    @patch("mcp_server.seeding.examples.RagEngine")
    @patch("mcp_server.seeding.examples.format_vector_for_postgres")
    async def test_uses_question_when_summary_is_none(self, mock_format, mock_rag, mock_db):
        """Test fallback to question when summary is NULL."""
        from mcp_server.seeding.examples import generate_missing_embeddings

        mock_rows = [{"id": 1, "question": "Test question?", "summary": None}]

        mock_conn = AsyncMock()
        mock_conn.fetch = AsyncMock(return_value=mock_rows)
        mock_conn.execute = AsyncMock()

        mock_context = AsyncMock()
        mock_context.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_context.__aexit__ = AsyncMock(return_value=None)
        mock_db.get_connection.return_value = mock_context

        mock_rag.embed_text.return_value = [0.1]
        mock_format.return_value = "[0.1]"

        await generate_missing_embeddings()

        # Should use question when summary is None
        mock_rag.embed_text.assert_called_once_with("Test question?")
