"""Unit tests for Memgraph vector index DDL utility."""

from unittest.mock import MagicMock

import pytest
from mcp_server.services.ingestion.vector_index_ddl import ensure_table_embedding_hnsw_index


class TestVectorIndexDDL:
    """Tests for vector index creation utility."""

    def test_ensure_index_creates_successfully(self):
        """Verify DDL is correct and returns True on success."""
        mock_session = MagicMock()

        result = ensure_table_embedding_hnsw_index(mock_session)

        assert result is True
        mock_session.run.assert_called_once()
        query = mock_session.run.call_args[0][0]

        # Verify DDL syntax
        assert "CREATE VECTOR INDEX table_embedding_index" in query
        assert "ON :Table(embedding)" in query
        assert "'dimension': 1536" in query
        assert "'metric': 'cosine'" in query

    def test_ensure_index_already_exists(self):
        """Verify returns False (and suppresses error) if index already exists."""
        mock_session = MagicMock()
        # Simulate Memgraph "already exists" error
        mock_session.run.side_effect = Exception(
            "Neo.ClientError.Schema.IndexAlreadyExists: The index already exists"
        )

        result = ensure_table_embedding_hnsw_index(mock_session)

        assert result is False
        mock_session.run.assert_called_once()

    def test_ensure_index_propagates_unexpected_error(self):
        """Verify unexpected errors are re-raised."""
        mock_session = MagicMock()
        mock_session.run.side_effect = Exception("SyntaxError: Invalid syntax")

        with pytest.raises(Exception, match="SyntaxError"):
            ensure_table_embedding_hnsw_index(mock_session)

    def test_custom_dimensions(self):
        """Verify custom dimensions can be passed."""
        mock_session = MagicMock()

        ensure_table_embedding_hnsw_index(mock_session, dims=768)

        query = mock_session.run.call_args[0][0]
        assert "'dimension': 768" in query
