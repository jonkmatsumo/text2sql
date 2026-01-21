"""Tests for list_interactions tool."""

from unittest.mock import AsyncMock, patch

import pytest

from mcp_server.tools.admin.list_interactions import TOOL_NAME, handler


class TestListInteractions:
    """Tests for list_interactions tool."""

    def test_tool_name_no_suffix(self):
        """Verify TOOL_NAME does not end with '_tool'."""
        assert not TOOL_NAME.endswith("_tool")
        assert TOOL_NAME == "list_interactions"

    @pytest.mark.asyncio
    async def test_list_interactions_returns_list(self):
        """Test list_interactions returns interaction list."""
        mock_interactions = [
            {"id": "int-1", "user_nlq_text": "query 1"},
            {"id": "int-2", "user_nlq_text": "query 2"},
        ]

        with patch(
            "mcp_server.tools.admin.list_interactions.get_interaction_store"
        ) as mock_get_store:
            mock_store = AsyncMock()
            mock_store.get_recent_interactions = AsyncMock(return_value=mock_interactions)
            mock_get_store.return_value = mock_store

            result = await handler(limit=10, offset=0)

            mock_store.get_recent_interactions.assert_called_once_with(10, 0)
            assert result == mock_interactions

    @pytest.mark.asyncio
    async def test_list_interactions_default_params(self):
        """Test default parameter values."""
        with patch(
            "mcp_server.tools.admin.list_interactions.get_interaction_store"
        ) as mock_get_store:
            mock_store = AsyncMock()
            mock_store.get_recent_interactions = AsyncMock(return_value=[])
            mock_get_store.return_value = mock_store

            await handler()

            # Default limit=50, offset=0
            mock_store.get_recent_interactions.assert_called_once_with(50, 0)
