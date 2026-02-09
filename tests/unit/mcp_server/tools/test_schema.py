"""Tests for schema tools (get_sample_data)."""

import json
from unittest.mock import AsyncMock, patch

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
    async def test_get_sample_data(self):
        """Test get_sample_data tool."""
        mock_conn = AsyncMock()
        mock_conn.fetch = AsyncMock(return_value=[{"a": 1}])
        mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_conn.__aexit__ = AsyncMock(return_value=False)

        with (
            patch("dal.database.Database.get_connection", return_value=mock_conn),
            patch("dal.database.Database.get_query_target_provider", return_value="duckdb"),
        ):
            result_json = await get_sample_data("t1", limit=5, tenant_id=1)
            response = json.loads(result_json)
            result = response["result"]

            assert len(result) == 1
            assert result[0] == {"a": 1}
            mock_conn.fetch.assert_called()
