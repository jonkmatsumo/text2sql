"""Test suite for refactored legacy tools.

These tests verify the tools use the expected DAL/store implementations.
"""

import json
from unittest.mock import AsyncMock, patch

import mcp_server.tools.get_table_schema as get_table_schema_mod
import mcp_server.tools.list_tables as list_tables_mod
import mcp_server.tools.search_relevant_tables as search_relevant_tables_mod
import pytest
from mcp_server.models import ColumnDef, TableDef

get_table_schema = get_table_schema_mod.handler
list_tables = list_tables_mod.handler
search_relevant_tables = search_relevant_tables_mod.handler


class TestLegacyTools:
    """Test suite for refactored legacy tools."""

    @pytest.mark.asyncio
    async def test_list_tables(self):
        """Test list_tables uses MetadataStore."""
        mock_store = AsyncMock()
        mock_store.list_tables.return_value = ["users", "orders"]

        with patch.object(list_tables_mod.Database, "get_metadata_store", return_value=mock_store):
            result = await list_tables(search_term="order")

            mock_store.list_tables.assert_called_once()
            assert "orders" in result
            assert "users" not in result

    @pytest.mark.asyncio
    async def test_get_table_schema(self):
        """Test get_table_schema uses MetadataStore."""
        mock_store = AsyncMock()
        mock_store.get_table_definition.return_value = json.dumps(
            {"table_name": "t1", "columns": []}
        )

        with patch.object(
            get_table_schema_mod.Database, "get_metadata_store", return_value=mock_store
        ):
            result = await get_table_schema(["t1"])

            mock_store.get_table_definition.assert_called_once_with("t1")
            data = json.loads(result)
            assert len(data) == 1
            assert data[0]["table_name"] == "t1"

    @pytest.mark.asyncio
    async def test_search_relevant_tables(self):
        """Test search_relevant_tables uses SchemaIntrospector."""
        mock_introspector = AsyncMock()
        mock_introspector.get_table_def.return_value = TableDef(
            name="t1",
            columns=[ColumnDef(name="c1", data_type="int", is_nullable=False)],
            foreign_keys=[],
        )

        mock_search = [{"table_name": "t1", "schema_text": "text", "distance": 0.1}]

        with patch.object(
            search_relevant_tables_mod.RagEngine, "embed_text", return_value=[0.1]
        ), patch.object(
            search_relevant_tables_mod,
            "search_similar_tables",
            new_callable=AsyncMock,
            return_value=mock_search,
        ), patch.object(
            search_relevant_tables_mod.Database,
            "get_schema_introspector",
            return_value=mock_introspector,
        ):

            result = await search_relevant_tables("query")

            mock_introspector.get_table_def.assert_called_once_with("t1")
            data = json.loads(result)
            assert data[0]["table_name"] == "t1"
            assert data[0]["columns"][0]["name"] == "c1"
