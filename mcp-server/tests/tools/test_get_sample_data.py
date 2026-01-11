"""Tests for get_sample_data tool."""

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from mcp_server.tools.get_sample_data import TOOL_NAME, handler


class TestGetSampleData:
    """Tests for get_sample_data tool."""

    def test_tool_name_no_suffix(self):
        """Verify TOOL_NAME does not end with '_tool'."""
        assert not TOOL_NAME.endswith("_tool")
        assert TOOL_NAME == "get_sample_data"

    @pytest.mark.asyncio
    @patch("mcp_server.tools.get_sample_data.get_schema_introspector")
    async def test_get_sample_data_returns_rows(self, mock_get_introspector):
        """Test get_sample_data returns sample rows."""
        mock_introspector = MagicMock()
        mock_introspector.get_sample_rows = AsyncMock(return_value=[{"id": 1, "name": "test"}])
        mock_get_introspector.return_value = mock_introspector

        result = await handler("users", limit=5)

        mock_introspector.get_sample_rows.assert_called_once_with("users", 5)
        data = json.loads(result)
        assert len(data) == 1
        assert data[0]["id"] == 1

    @pytest.mark.asyncio
    @patch("mcp_server.tools.get_sample_data.get_schema_introspector")
    async def test_get_sample_data_default_limit(self, mock_get_introspector):
        """Test default limit value."""
        mock_introspector = MagicMock()
        mock_introspector.get_sample_rows = AsyncMock(return_value=[])
        mock_get_introspector.return_value = mock_introspector

        await handler("users")

        # Default limit=3
        mock_introspector.get_sample_rows.assert_called_once_with("users", 3)

    @pytest.mark.asyncio
    @patch("mcp_server.tools.get_sample_data.get_schema_introspector")
    async def test_get_sample_data_empty_table(self, mock_get_introspector):
        """Test get_sample_data with empty table."""
        mock_introspector = MagicMock()
        mock_introspector.get_sample_rows = AsyncMock(return_value=[])
        mock_get_introspector.return_value = mock_introspector

        result = await handler("empty_table")

        data = json.loads(result)
        assert data == []
