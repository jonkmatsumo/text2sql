"""Unit tests for Memgraph vector index DDL utility."""

from unittest.mock import MagicMock, patch

import pytest

from common.interfaces import GraphStore
from ingestion.vector_index_ddl import ensure_table_embedding_hnsw_index


class TestVectorIndexDDL:
    """Tests for vector index creation utility."""

    @patch("ingestion.vector_index_ddl.logger")
    def test_ensure_index_creates_successfully(self, mock_logger):
        """Verify DDL is correct and returns True on success."""
        mock_store = MagicMock(spec=GraphStore)

        result = ensure_table_embedding_hnsw_index(mock_store)

        assert result is True
        mock_store.run_query.assert_called_once()
        query = mock_store.run_query.call_args[0][0]

        # Verify DDL syntax
        assert "CREATE VECTOR INDEX table_embedding_index" in query
        assert "ON :Table(embedding)" in query
        assert "'dimension': 1536" in query
        assert "'metric': 'cosine'" in query

        # Verify structured log
        assert mock_logger.info.call_count >= 2
        calls = mock_logger.info.call_args_list
        success_call = calls[-1]
        assert "Created vector index" in success_call[0][0]
        assert "extra" in success_call[1]
        extra = success_call[1]["extra"]
        assert extra["event"] == "memgraph_vector_index_ensure"
        assert extra["created"] is True
        assert "elapsed_ms" in extra

    @patch("ingestion.vector_index_ddl.logger")
    def test_ensure_index_already_exists(self, mock_logger):
        """Verify returns False (and suppresses error) if index already exists."""
        mock_store = MagicMock(spec=GraphStore)
        # Simulate Memgraph "already exists" error
        mock_store.run_query.side_effect = Exception(
            "Neo.ClientError.Schema.IndexAlreadyExists: The index already exists"
        )

        result = ensure_table_embedding_hnsw_index(mock_store)

        assert result is False
        mock_store.run_query.assert_called_once()

        # Verify debug log for existing index
        mock_logger.debug.assert_called_once()
        args, kwargs = mock_logger.debug.call_args
        assert "already exists" in args[0]
        assert kwargs["extra"]["created"] is False
        assert kwargs["extra"]["reason"] == "already_exists"

    @patch("ingestion.vector_index_ddl.logger")
    def test_ensure_index_propagates_unexpected_error(self, mock_logger):
        """Verify unexpected errors are re-raised."""
        mock_store = MagicMock(spec=GraphStore)
        mock_store.run_query.side_effect = Exception("SyntaxError: Invalid syntax")

        with pytest.raises(Exception, match="SyntaxError"):
            ensure_table_embedding_hnsw_index(mock_store)

        # Verify error log
        mock_logger.error.assert_called_once()
        args, kwargs = mock_logger.error.call_args
        assert kwargs["extra"]["event"] == "memgraph_vector_index_failure"

    def test_custom_dimensions(self):
        """Verify custom dimensions can be passed."""
        mock_store = MagicMock(spec=GraphStore)

        ensure_table_embedding_hnsw_index(mock_store, dims=768)

        query = mock_store.run_query.call_args[0][0]
        assert "'dimension': 768" in query
