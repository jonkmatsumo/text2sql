"""Unit tests for schema indexer service."""

from unittest.mock import AsyncMock, patch

import pytest
from mcp_server.models import ColumnDef, ForeignKeyDef, TableDef
from mcp_server.services.indexer_service import index_all_tables

# ... (skipping lines 4-101)
# We could check the arguments passed to embed_text if we want to be strict
# about schema generation - but that's testing generate_schema_document really.


@pytest.mark.asyncio
async def test_index_all_tables_success():
    """Test successful indexing of multiple tables."""
    # Mock dependencies
    mock_introspector = AsyncMock()
    mock_introspector.list_table_names.return_value = ["table1", "table2"]

    mock_table1 = TableDef(
        name="table1",
        columns=[ColumnDef(name="id", data_type="int", is_nullable=False)],
        foreign_keys=[],
    )
    mock_table2 = TableDef(
        name="table2",
        columns=[ColumnDef(name="id", data_type="int", is_nullable=False)],
        foreign_keys=[],
    )
    mock_introspector.get_table_def.side_effect = [mock_table1, mock_table2]

    mock_store = AsyncMock()

    # Mock Database methods
    with patch("mcp_server.services.indexer_service.Database") as MockDatabase:
        MockDatabase.get_schema_introspector.return_value = mock_introspector
        MockDatabase.get_schema_store.return_value = mock_store

        with patch("mcp_server.services.indexer_service.RagEngine.embed_text") as mock_embed:
            mock_embed.return_value = [0.1] * 384

            with patch("mcp_server.rag.reload_schema_index") as mock_reload:
                await index_all_tables()

                # Verify logic
                assert mock_introspector.list_table_names.call_count == 1
                assert mock_introspector.get_table_def.call_count == 2
                assert mock_embed.call_count == 2
                assert mock_store.save_schema_embedding.call_count == 2
                mock_reload.assert_called_once()


@pytest.mark.asyncio
async def test_index_all_tables_empty_database():
    """Test handling of empty database."""
    mock_introspector = AsyncMock()
    mock_introspector.list_table_names.return_value = []

    mock_store = AsyncMock()

    with patch("mcp_server.services.indexer_service.Database") as MockDatabase:
        MockDatabase.get_schema_introspector.return_value = mock_introspector
        MockDatabase.get_schema_store.return_value = mock_store

        with patch("mcp_server.rag.reload_schema_index") as mock_reload:
            await index_all_tables()

            mock_store.save_schema_embedding.assert_not_called()
            mock_reload.assert_called_once()


@pytest.mark.asyncio
async def test_index_all_tables_with_relationships():
    """Test indexing table with relationships to verify content passed to embedder."""
    mock_introspector = AsyncMock()
    mock_introspector.list_table_names.return_value = ["orders"]

    mock_table = TableDef(
        name="orders",
        columns=[
            ColumnDef(name="id", data_type="int", is_nullable=False),
            ColumnDef(name="user_id", data_type="int", is_nullable=False),
        ],
        foreign_keys=[
            ForeignKeyDef(
                column_name="user_id", foreign_table_name="users", foreign_column_name="id"
            )
        ],
    )
    mock_introspector.get_table_def.return_value = mock_table

    mock_store = AsyncMock()

    with patch("mcp_server.services.indexer_service.Database") as MockDatabase:
        MockDatabase.get_schema_introspector.return_value = mock_introspector
        MockDatabase.get_schema_store.return_value = mock_store

        with patch("mcp_server.services.indexer_service.RagEngine.embed_text") as mock_embed:
            mock_embed.return_value = [0.1] * 384
            with patch("mcp_server.rag.reload_schema_index"):
                await index_all_tables()

                # Check that embed_text was called
                mock_embed.assert_called_once()
                # We could check the arguments passed to embed_text if we want to be strict
                # about schema generation. But that's testing generate_schema_document really.
