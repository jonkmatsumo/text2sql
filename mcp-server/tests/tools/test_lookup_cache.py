"""Tests for lookup_cache tool."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from mcp_server.tools.lookup_cache import TOOL_NAME, handler


class TestLookupCache:
    """Tests for lookup_cache tool."""

    def test_tool_name_no_suffix(self):
        """Verify TOOL_NAME does not end with '_tool'."""
        assert not TOOL_NAME.endswith("_tool")
        assert TOOL_NAME == "lookup_cache"

    @pytest.mark.asyncio
    async def test_lookup_cache_hit(self):
        """Test cache hit returns cached value."""
        mock_result = MagicMock()
        mock_result.model_dump.return_value = {"sql": "SELECT * FROM users", "confidence": 0.95}

        with patch(
            "mcp_server.tools.lookup_cache.lookup_cache_svc", new_callable=AsyncMock
        ) as mock_svc:
            mock_svc.return_value = mock_result

            result = await handler("show all users", tenant_id=1)

            mock_svc.assert_called_once_with("show all users", 1)
            assert "SELECT * FROM users" in result

    @pytest.mark.asyncio
    async def test_lookup_cache_miss(self):
        """Test cache miss returns MISSING."""
        with patch(
            "mcp_server.tools.lookup_cache.lookup_cache_svc", new_callable=AsyncMock
        ) as mock_svc:
            mock_svc.return_value = None

            result = await handler("unknown query", tenant_id=1)

            mock_svc.assert_called_once_with("unknown query", 1)
            assert result == "MISSING"
