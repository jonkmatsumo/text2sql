from unittest.mock import AsyncMock, patch

import pytest
from mcp_server.models.database.column_def import ColumnDef
from mcp_server.models.database.table_def import TableDef
from mcp_server.services.indexer_service import index_all_tables


class TestIndexer:
    """Test suite for Schema Indexer."""

    @pytest.mark.asyncio
    async def test_index_all_tables(self):
        """Test indexing flow with mocked introspector and store."""
        mock_introspector = AsyncMock()
        mock_introspector.list_table_names.return_value = ["users"]

        mock_table_def = TableDef(
            name="users",
            columns=[
                ColumnDef(name="id", data_type="integer", is_nullable=False),
                ColumnDef(name="email", data_type="text", is_nullable=True),
            ],
            foreign_keys=[],
        )
        mock_introspector.get_table_def.return_value = mock_table_def

        # Mock Store
        mock_store = AsyncMock()

        # Mock Database
        with patch(
            "mcp_server.services.indexer_service.Database.get_schema_introspector",
            return_value=mock_introspector,
        ), patch(
            "mcp_server.services.indexer_service.Database.get_schema_store", return_value=mock_store
        ), patch(
            "mcp_server.rag.RagEngine.embed_text", return_value=[0.1, 0.2]
        ), patch(
            "mcp_server.rag.reload_schema_index", new_callable=AsyncMock
        ) as mock_reload:

            await index_all_tables()

            # Verify Interactions
            mock_introspector.list_table_names.assert_called_once()
            mock_introspector.get_table_def.assert_called_once_with("users")

            mock_store.save_schema_embedding.assert_called_once()
            embedding_arg = mock_store.save_schema_embedding.call_args[0][0]
            assert embedding_arg.table_name == "users"
            assert "email (text, nullable)" in embedding_arg.schema_text
            assert embedding_arg.embedding == [0.1, 0.2]

            mock_reload.assert_called_once()
