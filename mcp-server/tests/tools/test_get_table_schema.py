"""Tests for get_table_schema tool."""

import json
from unittest.mock import AsyncMock, patch

import pytest
from mcp_server.tools.get_table_schema import TOOL_NAME, handler


class TestGetTableSchema:
    """Tests for get_table_schema tool."""

    def test_tool_name_no_suffix(self):
        """Verify TOOL_NAME does not end with '_tool'."""
        assert not TOOL_NAME.endswith("_tool")
        assert TOOL_NAME == "get_table_schema"

    @pytest.mark.asyncio
    async def test_get_table_schema_single_table(self):
        """Test get_table_schema with a single table."""
        mock_store = AsyncMock()
        mock_store.get_table_definition.return_value = json.dumps(
            {
                "table_name": "users",
                "columns": [{"name": "id", "type": "integer"}],
                "foreign_keys": [],
            }
        )

        with patch(
            "mcp_server.tools.get_table_schema.Database.get_metadata_store", return_value=mock_store
        ):
            result = await handler(["users"])

            mock_store.get_table_definition.assert_called_once_with("users")
            data = json.loads(result)
            assert len(data) == 1
            assert data[0]["table_name"] == "users"

    @pytest.mark.asyncio
    async def test_get_table_schema_multiple_tables(self):
        """Test get_table_schema with multiple tables."""
        mock_store = AsyncMock()

        def get_def(table):
            return json.dumps(
                {
                    "table_name": table,
                    "columns": [{"name": "id", "type": "integer"}],
                    "foreign_keys": [],
                }
            )

        mock_store.get_table_definition.side_effect = get_def

        with patch(
            "mcp_server.tools.get_table_schema.Database.get_metadata_store", return_value=mock_store
        ):
            result = await handler(["users", "orders"])

            assert mock_store.get_table_definition.call_count == 2
            data = json.loads(result)
            assert len(data) == 2

    @pytest.mark.asyncio
    async def test_get_table_schema_skips_missing_table(self):
        """Test get_table_schema silently skips missing tables."""
        mock_store = AsyncMock()

        def get_def(table):
            if table == "missing":
                raise Exception("Table not found")
            return json.dumps({"table_name": table, "columns": [], "foreign_keys": []})

        mock_store.get_table_definition.side_effect = get_def

        with patch(
            "mcp_server.tools.get_table_schema.Database.get_metadata_store", return_value=mock_store
        ):
            result = await handler(["users", "missing", "orders"])

            data = json.loads(result)
            assert len(data) == 2
            assert data[0]["table_name"] == "users"
            assert data[1]["table_name"] == "orders"

    @pytest.mark.asyncio
    async def test_get_table_schema_empty_list(self):
        """Test get_table_schema with empty table list."""
        mock_store = AsyncMock()

        with patch(
            "mcp_server.tools.get_table_schema.Database.get_metadata_store", return_value=mock_store
        ):
            result = await handler([])

            mock_store.get_table_definition.assert_not_called()
            data = json.loads(result)
            assert data == []
