"""Tests for schema tools (get_sample_data)."""

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from mcp_server.tools.get_sample_data import TOOL_NAME as get_sample_data_tool_name
from mcp_server.tools.get_sample_data import handler as get_sample_data


class TestGetSampleData:
    """Tests for get_sample_data tool."""

    def test_tool_name_no_suffix(self):
        """Verify TOOL_NAME does not end with '_tool'."""
        assert not get_sample_data_tool_name.endswith("_tool")
        assert get_sample_data_tool_name == "get_sample_data"

    @pytest.mark.asyncio
    @patch("mcp_server.tools.get_sample_data.get_schema_introspector")
    async def test_get_sample_data(self, mock_get_introspector):
        """Test get_sample_data tool."""
        mock_introspector = MagicMock()
        mock_introspector.get_sample_rows = AsyncMock(return_value=[{"a": 1}])
        mock_get_introspector.return_value = mock_introspector

        result_json = await get_sample_data("t1", limit=5)
        response = json.loads(result_json)
        result = response["result"]

        assert len(result) == 1
        assert result[0] == {"a": 1}
        mock_introspector.get_sample_rows.assert_called_with("t1", 5)
