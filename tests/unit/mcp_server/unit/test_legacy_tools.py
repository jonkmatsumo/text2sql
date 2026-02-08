"""Test suite for refactored legacy tools.

These tests verify the tools use the expected DAL/store implementations.
"""

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import mcp_server.tools.get_table_schema as get_table_schema_mod
import mcp_server.tools.list_tables as list_tables_mod
import mcp_server.tools.search_relevant_tables as search_relevant_tables_mod
from schema import ColumnDef, TableDef

get_table_schema = get_table_schema_mod.handler
list_tables = list_tables_mod.handler
search_relevant_tables = search_relevant_tables_mod.handler


class TestLegacyTools:
    """Test suite for refactored legacy tools."""

    @pytest.mark.asyncio
    async def test_list_tables(self):
        """Test list_tables uses MetadataStore."""
        calls = {"count": 0}

        async def list_tables_async():
            calls["count"] += 1
            return ["users", "orders"]

        mock_store = MagicMock()
        mock_store.list_tables = list_tables_async

        with patch.object(list_tables_mod.Database, "get_metadata_store", return_value=mock_store):
            response = await list_tables(search_term="order")
            result = json.loads(response)["result"]

            assert calls["count"] == 1
            assert "orders" in result
            assert "users" not in result

    @pytest.mark.asyncio
    async def test_get_table_schema(self):
        """Test get_table_schema uses MetadataStore."""
        calls = {"tables": []}

        async def get_table_definition_async(table_name):
            calls["tables"].append(table_name)
            return json.dumps({"table_name": "t1", "columns": []})

        mock_store = MagicMock()
        mock_store.get_table_definition = get_table_definition_async

        with patch.object(
            get_table_schema_mod.Database, "get_metadata_store", return_value=mock_store
        ):
            result = await get_table_schema(["t1"])

            assert calls["tables"] == ["t1"]
            response = json.loads(result)
            data = response["result"]
            assert len(data) == 1
            assert data[0]["table_name"] == "t1"

    @pytest.mark.asyncio
    async def test_search_relevant_tables(self):
        """Test search_relevant_tables uses SchemaIntrospector."""
        calls = {"tables": []}

        async def get_table_def_async(table_name):
            calls["tables"].append(table_name)
            return TableDef(
                name="t1",
                columns=[ColumnDef(name="c1", data_type="int", is_nullable=False)],
                foreign_keys=[],
            )

        mock_introspector = MagicMock()
        mock_introspector.get_table_def = get_table_def_async

        mock_search = [{"table_name": "t1", "schema_text": "text", "distance": 0.1}]
        _fake_embed_text = AsyncMock(return_value=[0.1])

        async def _fake_search_similar_tables(_query_embedding, limit=5, tenant_id=None):
            return mock_search

        with (
            patch.object(search_relevant_tables_mod.RagEngine, "embed_text", new=_fake_embed_text),
            patch.object(
                search_relevant_tables_mod,
                "search_similar_tables",
                new=_fake_search_similar_tables,
            ),
            patch.object(
                search_relevant_tables_mod.Database,
                "get_schema_introspector",
                return_value=mock_introspector,
            ),
        ):

            result = await search_relevant_tables("query")

            assert calls["tables"] == ["t1"]
            assert calls["tables"] == ["t1"]
            data = json.loads(result)
            assert data[0]["table_name"] == "t1"
            assert data[0]["columns"][0]["name"] == "c1"
