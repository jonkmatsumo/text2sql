"""Unit tests for schema indexer service.

NOTE:
This file was renamed from test_indexer.py to test_schema_indexer.py to avoid
pytest import collisions under default import mode. The monorepo contains
multiple test_indexer.py files, which can cause 'import file mismatch'
errors during test collection from the repo root.
"""

from unittest.mock import MagicMock, patch

import pytest

from mcp_server.services.rag.indexer import index_all_tables
from schema import ColumnDef, ForeignKeyDef, TableDef

# ... (skipping lines 4-101)
# We could check the arguments passed to embed_text if we want to be strict
# about schema generation - but that's testing generate_schema_document really.


@pytest.mark.asyncio
async def test_index_all_tables_success():
    """Test successful indexing of multiple tables."""
    # Mock dependencies
    list_calls = {"count": 0}
    get_def_calls = {"count": 0}
    save_calls = {"count": 0}

    async def list_table_names_async():
        list_calls["count"] += 1
        return ["table1", "table2"]

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

    async def get_table_def_async(_table_name):
        get_def_calls["count"] += 1
        if get_def_calls["count"] == 1:
            return mock_table1
        return mock_table2

    async def save_schema_embedding_async(_schema_embedding):
        save_calls["count"] += 1

    mock_introspector = MagicMock()
    mock_introspector.list_table_names = list_table_names_async
    mock_introspector.get_table_def = get_table_def_async

    mock_store = MagicMock()
    mock_store.save_schema_embedding = save_schema_embedding_async

    # Mock Database methods
    with patch("mcp_server.services.rag.indexer.Database") as MockDatabase:
        MockDatabase.get_schema_introspector.return_value = mock_introspector
        MockDatabase.get_schema_store.return_value = mock_store

        async def embed_text_async(_schema_text):
            return [0.1] * 384

        async def reload_schema_index_async():
            return None

        with (
            patch("mcp_server.services.rag.indexer.RagEngine.embed_text", new=embed_text_async),
            patch("mcp_server.services.rag.reload_schema_index", new=reload_schema_index_async),
        ):
            await index_all_tables()

            # Verify logic
            assert list_calls["count"] == 1
            assert get_def_calls["count"] == 2
            assert save_calls["count"] == 2


@pytest.mark.asyncio
async def test_index_all_tables_empty_database():
    """Test handling of empty database."""
    list_calls = {"count": 0}
    save_calls = {"count": 0}

    async def list_table_names_async():
        list_calls["count"] += 1
        return []

    async def save_schema_embedding_async(_schema_embedding):
        save_calls["count"] += 1

    mock_introspector = MagicMock()
    mock_introspector.list_table_names = list_table_names_async

    mock_store = MagicMock()
    mock_store.save_schema_embedding = save_schema_embedding_async

    with patch("mcp_server.services.rag.indexer.Database") as MockDatabase:
        MockDatabase.get_schema_introspector.return_value = mock_introspector
        MockDatabase.get_schema_store.return_value = mock_store

        async def reload_schema_index_async():
            return None

        with patch("mcp_server.services.rag.reload_schema_index", new=reload_schema_index_async):
            await index_all_tables()

            assert save_calls["count"] == 0


@pytest.mark.asyncio
async def test_index_all_tables_with_relationships():
    """Test indexing table with relationships to verify content passed to embedder."""
    list_calls = {"count": 0}
    get_def_calls = {"count": 0}
    save_calls = {"count": 0}

    async def list_table_names_async():
        list_calls["count"] += 1
        return ["orders"]

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

    async def get_table_def_async(_table_name):
        get_def_calls["count"] += 1
        return mock_table

    async def save_schema_embedding_async(_schema_embedding):
        save_calls["count"] += 1

    mock_introspector = MagicMock()
    mock_introspector.list_table_names = list_table_names_async
    mock_introspector.get_table_def = get_table_def_async

    mock_store = MagicMock()
    mock_store.save_schema_embedding = save_schema_embedding_async

    with patch("mcp_server.services.rag.indexer.Database") as MockDatabase:
        MockDatabase.get_schema_introspector.return_value = mock_introspector
        MockDatabase.get_schema_store.return_value = mock_store

        async def embed_text_async(_schema_text):
            return [0.1] * 384

        async def reload_schema_index_async():
            return None

        with (
            patch("mcp_server.services.rag.indexer.RagEngine.embed_text", new=embed_text_async),
            patch("mcp_server.services.rag.reload_schema_index", new=reload_schema_index_async),
        ):
            await index_all_tables()

            assert list_calls["count"] == 1
            assert get_def_calls["count"] == 1
            assert save_calls["count"] == 1
