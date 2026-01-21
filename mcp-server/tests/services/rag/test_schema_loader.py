from unittest.mock import AsyncMock, MagicMock, patch

import numpy as np
import pytest

from mcp_server.services.rag.schema_loader import SchemaLoader


class TestSchemaLoader:
    """Test suite for SchemaLoader."""

    @pytest.mark.asyncio
    async def test_load_schema_embeddings(self):
        """Test loading schemas from store and adding to index."""
        mock_store = AsyncMock()
        mock_schemas = [
            MagicMock(table_name="users", schema_text="Table users...", embedding=[0.1, 0.2]),
            MagicMock(table_name="orders", schema_text="Table orders...", embedding=[0.3, 0.4]),
        ]
        mock_store.fetch_schema_embeddings.return_value = mock_schemas

        mock_index = MagicMock()

        with patch(
            "mcp_server.services.rag.schema_loader.Database.get_schema_store",
            return_value=mock_store,
        ):
            loader = SchemaLoader()
            await loader.load_schema_embeddings(mock_index)

            mock_index.add_items.assert_called_once()
            args, kwargs = mock_index.add_items.call_args

            vectors = args[0]
            ids = args[1]
            metadata = kwargs.get("metadata")

            assert len(ids) == 2
            assert ids == ["users", "orders"]
            assert isinstance(vectors, np.ndarray)
            assert vectors.shape == (2, 2)
            assert metadata["users"]["schema_text"] == "Table users..."

    @pytest.mark.asyncio
    async def test_load_schemas_empty(self):
        """Test handling empty store results."""
        mock_store = AsyncMock()
        mock_store.fetch_schema_embeddings.return_value = []

        mock_index = MagicMock()

        with patch(
            "mcp_server.services.rag.schema_loader.Database.get_schema_store",
            return_value=mock_store,
        ):
            loader = SchemaLoader()
            await loader.load_schema_embeddings(mock_index)

            mock_index.add_items.assert_not_called()
