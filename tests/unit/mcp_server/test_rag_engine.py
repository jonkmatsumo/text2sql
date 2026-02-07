"""Tests for RAG Engine."""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import numpy as np
import pytest

from mcp_server.services.rag.engine import (
    RagEngine,
    format_vector_for_postgres,
    generate_schema_document,
    search_similar_tables,
)


class TestRagEngine:
    """Unit tests for RagEngine class."""

    @pytest.fixture(autouse=True)
    def reset_model(self):
        """Reset the singleton model before each test."""
        RagEngine._model = None
        yield
        RagEngine._model = None

    @pytest.mark.asyncio
    async def test_model_lazy_loading(self):
        """Test that the embedding model is loaded only once."""
        with patch("mcp_server.services.rag.engine.TextEmbedding") as mock_embedding:
            mock_embedding.return_value = MagicMock()

            # First call loads model
            RagEngine._get_model()
            mock_embedding.assert_called_once()

            # Second call uses cached model
            RagEngine._get_model()
            mock_embedding.assert_called_once()

    @pytest.mark.asyncio
    async def test_embed_text_dimension(self):
        """Test that embed_text returns a vector of the correct dimension."""
        with patch("mcp_server.services.rag.engine.TextEmbedding") as mock_embedding:
            mock_model = MagicMock()
            mock_model.embed.return_value = iter([np.random.rand(384)])
            mock_embedding.return_value = mock_model

            embedding = await RagEngine.embed_text("test")
            assert len(embedding) == 384
            assert isinstance(embedding, list)

    @pytest.mark.asyncio
    async def test_embed_text_type(self):
        """Test that embed_text returns a list of floats."""
        with patch("mcp_server.services.rag.engine.TextEmbedding") as mock_embedding:
            mock_model = MagicMock()
            mock_model.embed.return_value = iter([np.random.rand(384)])
            mock_embedding.return_value = mock_model

            embedding = await RagEngine.embed_text("test")
            assert all(isinstance(x, (float, np.float32, np.float64)) for x in embedding)

    @pytest.mark.asyncio
    async def test_embed_batch(self):
        """Test that embed_batch returns multiple embeddings."""
        with patch("mcp_server.services.rag.engine.TextEmbedding") as mock_embedding:
            mock_model = MagicMock()
            mock_model.embed.return_value = iter([np.random.rand(384), np.random.rand(384)])
            mock_embedding.return_value = mock_model

            texts = ["test1", "test2"]
            embeddings = await RagEngine.embed_batch(texts)
            assert len(embeddings) == 2
            assert len(embeddings[0]) == 384
            assert len(embeddings[1]) == 384

    @pytest.mark.asyncio
    async def test_embed_batch_empty(self):
        """Test embed_batch with empty list."""
        with patch("mcp_server.services.rag.engine.TextEmbedding") as mock_embedding:
            mock_model = MagicMock()
            mock_model.embed.return_value = iter([])
            mock_embedding.return_value = mock_model

            embeddings = await RagEngine.embed_batch([])
            assert len(embeddings) == 0

    @pytest.mark.asyncio
    async def test_embed_text_empty_string(self):
        """Test embed_text with empty string."""
        with patch("mcp_server.services.rag.engine.TextEmbedding") as mock_embedding:
            mock_model = MagicMock()
            mock_model.embed.return_value = iter([np.zeros(384)])
            mock_embedding.return_value = mock_model

            embedding = await RagEngine.embed_text("")
            assert len(embedding) == 384


class TestFormatVectorForPostgres:
    """Unit tests for format_vector_for_postgres function."""

    def test_format_vector_basic(self):
        """Test formatting a simple vector."""
        embedding = [0.1, 0.2, 0.3]
        result = format_vector_for_postgres(embedding)
        assert result == "[0.1,0.2,0.3]"

    def test_format_vector_384d(self):
        """Test formatting a 384-dimension vector."""
        embedding = [0.1] * 384
        result = format_vector_for_postgres(embedding)
        assert result.startswith("[0.1,0.1,")
        assert result.endswith(",0.1]")
        assert result.count(",") == 383

    def test_format_vector_negative_values(self):
        """Test formatting vector with negative values."""
        embedding = [-0.1, 0.0, 0.1]
        result = format_vector_for_postgres(embedding)
        assert result == "[-0.1,0.0,0.1]"

    def test_format_vector_scientific_notation(self):
        """Test formatting vector with very small values."""
        embedding = [1e-10, 2e-10]
        result = format_vector_for_postgres(embedding)
        # float to string might use scientific notation
        assert "1e-10" in result or "0.0000000001" in result


class TestGenerateSchemaDocument:
    """Unit tests for generate_schema_document function."""

    def test_generate_schema_document_basic(self, monkeypatch):
        """Test basic table with columns."""
        monkeypatch.setenv("DAL_EXPERIMENTAL_FEATURES", "on")
        columns = [
            {"column_name": "id", "data_type": "integer", "is_nullable": "NO"},
            {"column_name": "name", "data_type": "text", "is_nullable": "YES"},
        ]
        doc = generate_schema_document("customer", columns)

        assert "Table: customer" in doc
        assert "id (int, identifier, required)" in doc
        assert "name (string, nullable)" in doc
        assert doc.endswith(".")

    def test_generate_schema_document_with_fks(self, monkeypatch):
        """Test with foreign keys."""
        monkeypatch.setenv("DAL_EXPERIMENTAL_FEATURES", "on")
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

    def test_generate_schema_document_no_fks(self, monkeypatch):
        """Test without foreign keys."""
        monkeypatch.setenv("DAL_EXPERIMENTAL_FEATURES", "on")
        columns = [
            {"column_name": "id", "data_type": "integer", "is_nullable": "NO"},
        ]
        doc = generate_schema_document("test_table", columns)

        assert "Table: test_table" in doc
        assert "Relationships:" not in doc

    def test_generate_schema_document_nullable_columns(self, monkeypatch):
        """Test nullable vs not null columns."""
        monkeypatch.setenv("DAL_EXPERIMENTAL_FEATURES", "on")
        columns = [
            {"column_name": "id", "data_type": "integer", "is_nullable": "NO"},
            {"column_name": "email", "data_type": "text", "is_nullable": "YES"},
        ]
        doc = generate_schema_document("user", columns)

        assert "id (int, identifier, required)" in doc
        assert "email (string, nullable)" in doc

    def test_generate_schema_document_empty_columns(self, monkeypatch):
        """Test edge case with empty columns list."""
        monkeypatch.setenv("DAL_EXPERIMENTAL_FEATURES", "on")
        doc = generate_schema_document("empty_table", [])

        assert "Table: empty_table" in doc
        assert "Columns:" in doc

    def test_generate_schema_document_with_table_comment(self, monkeypatch):
        """Test with table comment."""
        monkeypatch.setenv("DAL_EXPERIMENTAL_FEATURES", "on")
        columns = [
            {"column_name": "id", "data_type": "integer", "is_nullable": "NO"},
        ]
        doc = generate_schema_document(
            "payment", columns, table_comment="Customer payment transactions"
        )

        assert "Table payment: Customer payment transactions" in doc
        assert doc.startswith("Table payment:")

    def test_generate_schema_document_semantic_hints(self, monkeypatch):
        """Test semantic hint detection for various column names."""
        monkeypatch.setenv("DAL_EXPERIMENTAL_FEATURES", "on")
        columns = [
            {"column_name": "payment_id", "data_type": "integer", "is_nullable": "NO"},
            {"column_name": "amount", "data_type": "numeric", "is_nullable": "NO"},
            {"column_name": "payment_date", "data_type": "timestamp", "is_nullable": "NO"},
            {"column_name": "created_at", "data_type": "timestamp", "is_nullable": "NO"},
            {"column_name": "price", "data_type": "numeric", "is_nullable": "YES"},
            {"column_name": "name", "data_type": "text", "is_nullable": "NO"},
        ]
        doc = generate_schema_document("payment", columns)

        assert "payment_id (int, identifier, required)" in doc
        assert "amount (decimal, monetary value, required)" in doc
        assert "payment_date (timestamp, timestamp, required)" in doc
        assert "created_at (timestamp, required)" in doc
        assert "price (decimal, monetary value, nullable)" in doc
        assert "name (string, required)" in doc

    def test_generate_schema_document_backward_compatibility(self, monkeypatch):
        """Test backward compatibility (no new parameters)."""
        monkeypatch.setenv("DAL_EXPERIMENTAL_FEATURES", "on")
        columns = [
            {"column_name": "id", "data_type": "integer", "is_nullable": "NO"},
        ]
        # Call without table_comment (should work as before)
        doc = generate_schema_document("test_table", columns)

        assert "Table: test_table" in doc
        assert "id (int, identifier, required)" in doc

    def test_generate_schema_document_all_semantic_patterns(self, monkeypatch):
        """Test all semantic hint patterns."""
        monkeypatch.setenv("DAL_EXPERIMENTAL_FEATURES", "on")
        columns = [
            {"column_name": "user_id", "data_type": "integer", "is_nullable": "NO"},
            {"column_name": "order_id", "data_type": "integer", "is_nullable": "NO"},
            {"column_name": "transaction_date", "data_type": "date", "is_nullable": "NO"},
            {"column_name": "updated_time", "data_type": "timestamp", "is_nullable": "NO"},
            {"column_name": "total_amount", "data_type": "numeric", "is_nullable": "NO"},
            {"column_name": "unit_price", "data_type": "numeric", "is_nullable": "NO"},
        ]
        doc = generate_schema_document("orders", columns)

        assert "user_id (int, identifier, required)" in doc
        assert "order_id (int, identifier, required)" in doc
        assert "transaction_date (date, timestamp, required)" in doc
        assert "updated_time (timestamp, timestamp, required)" in doc
        assert "total_amount (decimal, monetary value, required)" in doc
        assert "unit_price (decimal, monetary value, required)" in doc

    def test_generate_schema_document_combination(self, monkeypatch):
        """Test combination of hints, relationships, and table comment."""
        monkeypatch.setenv("DAL_EXPERIMENTAL_FEATURES", "on")
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
        assert "payment_id (int, identifier, required)" in doc
        assert "amount (decimal, monetary value, required)" in doc
        assert "customer_id (int, identifier, required)" in doc
        assert "customer_id links to customer" in doc
        assert "Relationships:" in doc


class TestSearchSimilarTables:
    """Unit tests for search_similar_tables function."""

    @pytest.fixture(autouse=True)
    async def reset_schema_index(self):
        """Reset the singleton schema index before each test."""
        from mcp_server.services.rag import engine as rag_engine

        rag_engine._schema_index = None
        yield
        rag_engine._schema_index = None

    @pytest.mark.asyncio
    async def test_search_similar_tables_success(self):
        """Test successful search with results."""
        mock_index = MagicMock()
        mock_index.search.return_value = [
            SimpleNamespace(
                id="customer",
                score=0.9,
                metadata={"schema_text": "Table: customer. Columns: id, name"},
            ),
            SimpleNamespace(
                id="order",
                score=0.8,
                metadata={"schema_text": "Table: order. Columns: id, customer_id"},
            ),
        ]

        async def _fake_get_schema_index():
            return mock_index

        with patch("mcp_server.services.rag.engine._get_schema_index", new=_fake_get_schema_index):
            query_embedding = [0.1] * 384
            results = await search_similar_tables(query_embedding, limit=5)

            # Verify results
            assert len(results) == 2
            assert results[0]["table_name"] == "customer"
            assert pytest.approx(results[0]["distance"], abs=1e-5) == 0.1
            assert results[1]["table_name"] == "order"
            assert pytest.approx(results[1]["distance"], abs=1e-5) == 0.2

    @pytest.mark.asyncio
    async def test_search_similar_tables_empty_result(self):
        """Test empty result handling."""
        mock_index = MagicMock()
        mock_index.search.return_value = []

        async def _fake_get_schema_index():
            return mock_index

        with patch("mcp_server.services.rag.engine._get_schema_index", new=_fake_get_schema_index):
            query_embedding = [0.1] * 384
            results = await search_similar_tables(query_embedding, limit=5)

            assert results == []

    @pytest.mark.asyncio
    async def test_search_similar_tables_limit(self):
        """Test limit parameter."""
        mock_index = MagicMock()
        mock_index.search.side_effect = lambda _query_vector, k: [
            SimpleNamespace(
                id=f"table_{i}",
                score=0.9,
                metadata={"schema_text": f"text_{i}"},
            )
            for i in range(k)
        ]

        async def _fake_get_schema_index():
            return mock_index

        with patch("mcp_server.services.rag.engine._get_schema_index", new=_fake_get_schema_index):
            query_embedding = [0.1] * 384
            results = await search_similar_tables(query_embedding, limit=3)

            # Results should be limited (via index search logic)
            assert len(results) == 3
