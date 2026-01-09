"""Unit tests for RAG engine module."""

from unittest.mock import AsyncMock, MagicMock, patch

import numpy as np
import pytest
from mcp_server.dal.postgres import PostgresSchemaStore
from mcp_server.rag import (
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

        with patch("mcp_server.rag.TextEmbedding", return_value=mock_model):
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

        with patch("mcp_server.rag.TextEmbedding", return_value=mock_model):
            embedding = RagEngine.embed_text("test table with customer data")
            assert len(embedding) == 384

    def test_embed_text_type(self):
        """Test that return type is list[float]."""
        mock_model = MagicMock()
        mock_embedding = np.array([0.1, 0.2, 0.3] * 128)  # 384 elements
        mock_model.embed.return_value = iter([mock_embedding])

        with patch("mcp_server.rag.TextEmbedding", return_value=mock_model):
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

        with patch("mcp_server.rag.TextEmbedding", return_value=mock_model):
            texts = ["text1", "text2", "text3"]
            embeddings = RagEngine.embed_batch(texts)

            assert len(embeddings) == 3
            assert all(len(emb) == 384 for emb in embeddings)
            assert all(isinstance(emb, list) for emb in embeddings)

    def test_embed_batch_empty(self):
        """Test empty batch handling."""
        mock_model = MagicMock()
        mock_model.embed.return_value = iter([])

        with patch("mcp_server.rag.TextEmbedding", return_value=mock_model):
            embeddings = RagEngine.embed_batch([])
            assert embeddings == []

    def test_embed_text_empty_string(self):
        """Test empty string handling."""
        mock_model = MagicMock()
        mock_embedding = np.array([0.0] * 384)
        mock_model.embed.return_value = iter([mock_embedding])

        with patch("mcp_server.rag.TextEmbedding", return_value=mock_model):
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
        assert "id (integer, identifier, required)" in doc
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
        assert "customer_id links to customer" in doc
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

        assert "id (integer, identifier, required)" in doc
        assert "email (text, nullable)" in doc

    def test_generate_schema_document_empty_columns(self):
        """Test edge case with empty columns list."""
        doc = generate_schema_document("empty_table", [])

        assert "Table: empty_table" in doc
        assert "Columns:" in doc

    def test_generate_schema_document_with_table_comment(self):
        """Test with table comment."""
        columns = [
            {"column_name": "id", "data_type": "integer", "is_nullable": "NO"},
        ]
        doc = generate_schema_document(
            "payment", columns, table_comment="Customer payment transactions"
        )

        assert "Table payment: Customer payment transactions" in doc
        assert doc.startswith("Table payment:")

    def test_generate_schema_document_semantic_hints(self):
        """Test semantic hint detection for various column names."""
        columns = [
            {"column_name": "payment_id", "data_type": "integer", "is_nullable": "NO"},
            {"column_name": "amount", "data_type": "numeric", "is_nullable": "NO"},
            {"column_name": "payment_date", "data_type": "timestamp", "is_nullable": "NO"},
            {"column_name": "created_at", "data_type": "timestamp", "is_nullable": "NO"},
            {"column_name": "price", "data_type": "numeric", "is_nullable": "YES"},
            {"column_name": "name", "data_type": "text", "is_nullable": "NO"},
        ]
        doc = generate_schema_document("payment", columns)

        assert "payment_id (integer, identifier, required)" in doc
        assert "amount (numeric, monetary value, required)" in doc
        assert "payment_date (timestamp, timestamp, required)" in doc
        assert "created_at (timestamp, required)" in doc  # No semantic hint (doesn't match pattern)
        assert "price (numeric, monetary value, nullable)" in doc
        assert "name (text, required)" in doc  # No semantic hint

    def test_generate_schema_document_backward_compatibility(self):
        """Test backward compatibility (no new parameters)."""
        columns = [
            {"column_name": "id", "data_type": "integer", "is_nullable": "NO"},
        ]
        # Call without table_comment (should work as before)
        doc = generate_schema_document("test_table", columns)

        assert "Table: test_table" in doc
        assert "id (integer, identifier, required)" in doc

    def test_generate_schema_document_all_semantic_patterns(self):
        """Test all semantic hint patterns."""
        columns = [
            {"column_name": "user_id", "data_type": "integer", "is_nullable": "NO"},
            {"column_name": "order_id", "data_type": "integer", "is_nullable": "NO"},
            {"column_name": "transaction_date", "data_type": "date", "is_nullable": "NO"},
            {"column_name": "updated_time", "data_type": "timestamp", "is_nullable": "NO"},
            {"column_name": "total_amount", "data_type": "numeric", "is_nullable": "NO"},
            {"column_name": "unit_price", "data_type": "numeric", "is_nullable": "NO"},
        ]
        doc = generate_schema_document("orders", columns)

        # All should have semantic hints
        assert "identifier" in doc
        assert "timestamp" in doc
        assert "monetary value" in doc

    def test_generate_schema_document_combination(self):
        """Test combination of hints, relationships, and table comment."""
        columns = [
            {"column_name": "payment_id", "data_type": "integer", "is_nullable": "NO"},
            {"column_name": "customer_id", "data_type": "integer", "is_nullable": "NO"},
            {"column_name": "amount", "data_type": "numeric", "is_nullable": "NO"},
        ]
        foreign_keys = [
            {"column_name": "customer_id", "foreign_table_name": "customer"},
        ]
        doc = generate_schema_document(
            "payment",
            columns,
            foreign_keys,
            table_comment="Customer payment transactions",
        )

        assert "Table payment: Customer payment transactions" in doc
        assert "payment_id (integer, identifier, required)" in doc
        assert "amount (numeric, monetary value, required)" in doc
        assert "customer_id links to customer" in doc
        assert "Relationships:" in doc


class TestSearchSimilarTables:
    """Unit tests for search_similar_tables function."""

    @pytest.fixture(autouse=True)
    async def reset_schema_index(self):
        """Reset the singleton schema index before each test."""
        import mcp_server.rag

        mcp_server.rag._schema_index = None
        yield
        mcp_server.rag._schema_index = None

    @pytest.mark.asyncio
    async def test_search_similar_tables_success(self):
        """Test successful search with results."""
        mock_conn = AsyncMock()
        mock_rows = [
            {
                "table_name": "customer",
                "schema_text": "Table: customer. Columns: id, name",
                "distance": 0.1,
                "embedding": [0.1] * 384,
            },
            {
                "table_name": "order",
                "schema_text": "Table: order. Columns: id, customer_id",
                "distance": 0.2,
                "embedding": [0.2] * 384,
            },
        ]
        mock_conn.fetch = AsyncMock(return_value=mock_rows)

        # Setup async context manager mock
        mock_get_cm = AsyncMock()
        mock_get_cm.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_get_cm.__aexit__ = AsyncMock(return_value=False)
        mock_get = MagicMock(return_value=mock_get_cm)

        with patch("mcp_server.config.database.Database.get_connection", mock_get), patch(
            "mcp_server.config.database.Database.get_schema_store",
            return_value=PostgresSchemaStore(),
        ):
            query_embedding = [0.1] * 384
            results = await search_similar_tables(query_embedding, limit=5)

            # Verify connection was acquired (context manager called)
            mock_get.assert_called_once()

            # Verify results
            assert len(results) == 2
            assert results[0]["table_name"] == "customer"
            assert pytest.approx(results[0]["distance"], abs=1e-5) == 0.0
            assert results[1]["table_name"] == "order"
            assert pytest.approx(results[1]["distance"], abs=1e-5) == 0.0

    @pytest.mark.asyncio
    async def test_search_similar_tables_empty_result(self):
        """Test empty result handling."""
        mock_conn = AsyncMock()
        mock_conn.fetch = AsyncMock(return_value=[])

        # Setup async context manager mock
        mock_get_cm = AsyncMock()
        mock_get_cm.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_get_cm.__aexit__ = AsyncMock(return_value=False)
        mock_get = MagicMock(return_value=mock_get_cm)

        with patch("mcp_server.config.database.Database.get_connection", mock_get), patch(
            "mcp_server.config.database.Database.get_schema_store",
            return_value=PostgresSchemaStore(),
        ):
            query_embedding = [0.1] * 384
            results = await search_similar_tables(query_embedding, limit=5)

            assert results == []
            mock_get.assert_called_once()

    @pytest.mark.asyncio
    async def test_search_similar_tables_limit(self):
        """Test limit parameter."""
        mock_conn = AsyncMock()
        mock_rows = [
            {
                "table_name": f"table_{i}",
                "schema_text": f"text_{i}",
                "distance": float(i),
                "embedding": [0.1] * 384,
            }
            for i in range(10)
        ]
        mock_conn.fetch = AsyncMock(return_value=mock_rows)

        # Setup async context manager mock
        mock_get_cm = AsyncMock()
        mock_get_cm.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_get_cm.__aexit__ = AsyncMock(return_value=False)
        mock_get = MagicMock(return_value=mock_get_cm)

        with patch("mcp_server.config.database.Database.get_connection", mock_get), patch(
            "mcp_server.config.database.Database.get_schema_store",
            return_value=PostgresSchemaStore(),
        ):
            query_embedding = [0.1] * 384
            results = await search_similar_tables(query_embedding, limit=3)

            # Verify connection was acquired (context manager called)
            mock_get.assert_called_once()

            # Verify connection was acquired (context manager called)
            mock_get.assert_called_once()

            # Results should be limited (via index search logic)
            assert len(results) == 3
