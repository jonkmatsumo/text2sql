"""Unit tests for RAG engine module."""

from unittest.mock import AsyncMock, MagicMock, patch

import numpy as np
import pytest
from src.rag import (
    RagEngine,
    format_vector_for_postgres,
    generate_schema_document,
    search_similar_tables,
)


class TestRagEngine:
    """Unit tests for RagEngine class."""

    def setup_method(self):
        """Reset the model before each test."""
        RagEngine._model = None

    def test_model_lazy_loading(self):
        """Test that model loads on first use and reuses on subsequent calls."""
        mock_model = MagicMock()
        mock_embedding1 = np.array([0.1] * 384)
        mock_embedding2 = np.array([0.2] * 384)
        mock_model.embed.side_effect = [iter([mock_embedding1]), iter([mock_embedding2])]

        with patch("src.rag.TextEmbedding", return_value=mock_model):
            # First call should load the model
            embedding1 = RagEngine.embed_text("test text")
            assert RagEngine._model is not None
            assert mock_model.embed.call_count == 1
            assert len(embedding1) == 384

            # Second call should reuse the same model
            embedding2 = RagEngine.embed_text("another text")
            assert RagEngine._model is mock_model
            assert mock_model.embed.call_count == 2
            assert len(embedding2) == 384

    def test_embed_text_dimension(self):
        """Test that embedding is 384 dimensions."""
        mock_model = MagicMock()
        mock_embedding = np.array([0.1] * 384)
        mock_model.embed.return_value = iter([mock_embedding])

        with patch("src.rag.TextEmbedding", return_value=mock_model):
            embedding = RagEngine.embed_text("test table with customer data")
            assert len(embedding) == 384

    def test_embed_text_type(self):
        """Test that return type is list[float]."""
        mock_model = MagicMock()
        mock_embedding = np.array([0.1, 0.2, 0.3] * 128)  # 384 elements
        mock_model.embed.return_value = iter([mock_embedding])

        with patch("src.rag.TextEmbedding", return_value=mock_model):
            embedding = RagEngine.embed_text("test")
            assert isinstance(embedding, list)
            assert all(isinstance(x, float) for x in embedding)

    def test_embed_batch(self):
        """Test batch embedding with multiple texts."""
        mock_model = MagicMock()
        mock_embeddings = [
            np.array([0.1] * 384),
            np.array([0.2] * 384),
            np.array([0.3] * 384),
        ]
        mock_model.embed.return_value = iter(mock_embeddings)

        with patch("src.rag.TextEmbedding", return_value=mock_model):
            texts = ["text1", "text2", "text3"]
            embeddings = RagEngine.embed_batch(texts)

            assert len(embeddings) == 3
            assert all(len(emb) == 384 for emb in embeddings)
            assert all(isinstance(emb, list) for emb in embeddings)

    def test_embed_batch_empty(self):
        """Test empty batch handling."""
        mock_model = MagicMock()
        mock_model.embed.return_value = iter([])

        with patch("src.rag.TextEmbedding", return_value=mock_model):
            embeddings = RagEngine.embed_batch([])
            assert embeddings == []

    def test_embed_text_empty_string(self):
        """Test empty string handling."""
        mock_model = MagicMock()
        mock_embedding = np.array([0.0] * 384)
        mock_model.embed.return_value = iter([mock_embedding])

        with patch("src.rag.TextEmbedding", return_value=mock_model):
            embedding = RagEngine.embed_text("")
            assert len(embedding) == 384


class TestFormatVectorForPostgres:
    """Unit tests for format_vector_for_postgres function."""

    def test_format_vector_basic(self):
        """Test basic vector formatting."""
        vector = [1.0, 2.0, 3.0]
        result = format_vector_for_postgres(vector)
        assert result == "[1.0,2.0,3.0]"

    def test_format_vector_384d(self):
        """Test 384-dimensional vector formatting."""
        vector = [0.1] * 384
        result = format_vector_for_postgres(vector)
        assert result.startswith("[")
        assert result.endswith("]")
        assert result.count(",") == 383  # 384 elements = 383 commas

    def test_format_vector_negative_values(self):
        """Test negative float values."""
        vector = [-1.5, 2.0, -3.7]
        result = format_vector_for_postgres(vector)
        assert "-1.5" in result
        assert "-3.7" in result

    def test_format_vector_scientific_notation(self):
        """Test scientific notation handling."""
        vector = [1e-5, 2e10, 3.5]
        result = format_vector_for_postgres(vector)
        # Should convert to standard float format
        assert "1e-05" in result or "1.0e-05" in result or "0.00001" in result


class TestGenerateSchemaDocument:
    """Unit tests for generate_schema_document function."""

    def test_generate_schema_document_basic(self):
        """Test basic table with columns."""
        columns = [
            {"column_name": "id", "data_type": "integer", "is_nullable": "NO"},
            {"column_name": "name", "data_type": "text", "is_nullable": "YES"},
        ]
        doc = generate_schema_document("customer", columns)

        assert "Table: customer" in doc
        assert "id (integer, not null)" in doc
        assert "name (text, nullable)" in doc
        assert doc.endswith(".")

    def test_generate_schema_document_with_fks(self):
        """Test with foreign keys."""
        columns = [
            {"column_name": "id", "data_type": "integer", "is_nullable": "NO"},
            {"column_name": "customer_id", "data_type": "integer", "is_nullable": "NO"},
        ]
        foreign_keys = [
            {"column_name": "customer_id", "foreign_table_name": "customer"},
        ]
        doc = generate_schema_document("order", columns, foreign_keys)

        assert "Table: order" in doc
        assert "customer_id references customer" in doc
        assert "Relationships:" in doc

    def test_generate_schema_document_no_fks(self):
        """Test without foreign keys."""
        columns = [
            {"column_name": "id", "data_type": "integer", "is_nullable": "NO"},
        ]
        doc = generate_schema_document("test_table", columns)

        assert "Table: test_table" in doc
        assert "Relationships:" not in doc

    def test_generate_schema_document_nullable_columns(self):
        """Test nullable vs not null columns."""
        columns = [
            {"column_name": "id", "data_type": "integer", "is_nullable": "NO"},
            {"column_name": "email", "data_type": "text", "is_nullable": "YES"},
        ]
        doc = generate_schema_document("user", columns)

        assert "id (integer, not null)" in doc
        assert "email (text, nullable)" in doc

    def test_generate_schema_document_empty_columns(self):
        """Test edge case with empty columns list."""
        doc = generate_schema_document("empty_table", [])

        assert "Table: empty_table" in doc
        assert "Columns:" in doc


class TestSearchSimilarTables:
    """Unit tests for search_similar_tables function."""

    @pytest.mark.asyncio
    async def test_search_similar_tables_mock(self):
        """Test with mocked database."""
        mock_conn = AsyncMock()
        mock_rows = [
            {
                "table_name": "customer",
                "schema_text": "Table: customer. Columns: id, name",
                "distance": 0.1,
            },
            {
                "table_name": "order",
                "schema_text": "Table: order. Columns: id, customer_id",
                "distance": 0.2,
            },
        ]
        mock_conn.fetch = AsyncMock(return_value=mock_rows)

        with patch("src.rag.Database.get_connection", new_callable=AsyncMock) as mock_get:
            with patch(
                "src.rag.Database.release_connection", new_callable=AsyncMock
            ) as mock_release:
                mock_get.return_value = mock_conn

                query_embedding = [0.1] * 384
                results = await search_similar_tables(query_embedding, limit=5)

                # Verify connection was acquired and released
                mock_get.assert_called_once()
                mock_release.assert_called_once_with(mock_conn)

                # Verify query was executed
                mock_conn.fetch.assert_called_once()
                call_args = mock_conn.fetch.call_args[0]
                assert "SELECT" in call_args[0]
                assert "schema_embeddings" in call_args[0]
                assert "<=>" in call_args[0]  # Cosine distance operator

                # Verify results
                assert len(results) == 2
                assert results[0]["table_name"] == "customer"
                assert results[0]["distance"] == 0.1
                assert results[1]["table_name"] == "order"
                assert results[1]["distance"] == 0.2

    @pytest.mark.asyncio
    async def test_search_similar_tables_empty_result(self):
        """Test empty result handling."""
        mock_conn = AsyncMock()
        mock_conn.fetch = AsyncMock(return_value=[])

        with patch("src.rag.Database.get_connection", new_callable=AsyncMock) as mock_get:
            with patch(
                "src.rag.Database.release_connection", new_callable=AsyncMock
            ) as mock_release:
                mock_get.return_value = mock_conn

                query_embedding = [0.1] * 384
                results = await search_similar_tables(query_embedding, limit=5)

                assert results == []
                mock_release.assert_called_once_with(mock_conn)

    @pytest.mark.asyncio
    async def test_search_similar_tables_limit(self):
        """Test limit parameter."""
        mock_conn = AsyncMock()
        mock_rows = [
            {"table_name": f"table_{i}", "schema_text": f"text_{i}", "distance": float(i)}
            for i in range(10)
        ]
        mock_conn.fetch = AsyncMock(return_value=mock_rows)

        with patch("src.rag.Database.get_connection", new_callable=AsyncMock) as mock_get:
            with patch(
                "src.rag.Database.release_connection", new_callable=AsyncMock
            ) as mock_release:
                mock_get.return_value = mock_conn

                query_embedding = [0.1] * 384
                results = await search_similar_tables(query_embedding, limit=3)

                # Verify connection was acquired and released
                mock_get.assert_called_once()
                mock_release.assert_called_once_with(mock_conn)

                # Verify limit was passed to query
                call_args = mock_conn.fetch.call_args[0]
                assert call_args[2] == 3  # limit parameter

                # Results should be limited (though mock returns all)
                assert len(results) == 10  # Mock returns all, but in real DB it would be limited

    @pytest.mark.asyncio
    async def test_search_similar_tables_connection_cleanup(self):
        """Test connection always released even on error."""
        mock_conn = AsyncMock()
        mock_conn.fetch = AsyncMock(side_effect=Exception("Database error"))

        with patch("src.rag.Database.get_connection", new_callable=AsyncMock) as mock_get:
            with patch(
                "src.rag.Database.release_connection", new_callable=AsyncMock
            ) as mock_release:
                mock_get.return_value = mock_conn

                query_embedding = [0.1] * 384

                with pytest.raises(Exception):
                    await search_similar_tables(query_embedding, limit=5)

                # Connection should still be released
                mock_release.assert_called_once_with(mock_conn)

    @pytest.mark.asyncio
    async def test_search_similar_tables_query_structure(self):
        """Verify SQL query structure."""
        mock_conn = AsyncMock()
        mock_conn.fetch = AsyncMock(return_value=[])

        with patch("src.rag.Database.get_connection", new_callable=AsyncMock) as mock_get:
            with patch(
                "src.rag.Database.release_connection", new_callable=AsyncMock
            ) as mock_release:
                mock_get.return_value = mock_conn

                query_embedding = [0.1] * 384
                await search_similar_tables(query_embedding, limit=5)

                # Verify connection was acquired and released
                mock_get.assert_called_once()
                mock_release.assert_called_once_with(mock_conn)

                # Verify query structure
                call_args = mock_conn.fetch.call_args[0]
                query = call_args[0]
                assert "SELECT" in query
                assert "table_name" in query
                assert "schema_text" in query
                assert "distance" in query
                assert "schema_embeddings" in query
                assert "ORDER BY distance ASC" in query
                assert "LIMIT" in query
                assert "$1::vector" in query  # Parameterized query
                assert "$2" in query  # Limit parameter
