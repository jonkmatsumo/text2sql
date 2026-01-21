"""Tests for list_tables tool."""

import json
from unittest.mock import AsyncMock, patch

import pytest

from mcp_server.tools.list_tables import TOOL_NAME, handler


class TestListTables:
    """Tests for list_tables tool."""

    def test_tool_name_no_suffix(self):
        """Verify TOOL_NAME does not end with '_tool'."""
        assert not TOOL_NAME.endswith("_tool")
        assert TOOL_NAME == "list_tables"

    @pytest.mark.asyncio
    async def test_list_tables_returns_all_tables(self):
        """Test list_tables returns all tables when no filter."""
        mock_store = AsyncMock()
        mock_store.list_tables.return_value = ["users", "orders", "payments"]

        with patch(
            "mcp_server.tools.list_tables.Database.get_metadata_store", return_value=mock_store
        ):
            result = await handler()

            mock_store.list_tables.assert_called_once()
            data = json.loads(result)
            assert len(data) == 3
            assert "users" in data
            assert "orders" in data
            assert "payments" in data

    @pytest.mark.asyncio
    async def test_list_tables_with_search_filter(self):
        """Test list_tables filters by search term."""
        mock_store = AsyncMock()
        mock_store.list_tables.return_value = ["users", "orders", "user_sessions"]

        with patch(
            "mcp_server.tools.list_tables.Database.get_metadata_store", return_value=mock_store
        ):
            result = await handler(search_term="user")

            data = json.loads(result)
            assert len(data) == 2
            assert "users" in data
            assert "user_sessions" in data
            assert "orders" not in data

    @pytest.mark.asyncio
    async def test_list_tables_case_insensitive_search(self):
        """Test search is case insensitive."""
        mock_store = AsyncMock()
        mock_store.list_tables.return_value = ["Users", "ORDERS", "user_sessions"]

        with patch(
            "mcp_server.tools.list_tables.Database.get_metadata_store", return_value=mock_store
        ):
            result = await handler(search_term="USER")

            data = json.loads(result)
            assert len(data) == 2
            assert "Users" in data
            assert "user_sessions" in data

    @pytest.mark.asyncio
    async def test_list_tables_empty_result(self):
        """Test list_tables with no matching tables."""
        mock_store = AsyncMock()
        mock_store.list_tables.return_value = ["users", "orders"]

        with patch(
            "mcp_server.tools.list_tables.Database.get_metadata_store", return_value=mock_store
        ):
            result = await handler(search_term="nonexistent")

            data = json.loads(result)
            assert data == []
