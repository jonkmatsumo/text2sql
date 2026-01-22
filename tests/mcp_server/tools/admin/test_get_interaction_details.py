"""Tests for get_interaction_details tool."""

from unittest.mock import AsyncMock, patch

import pytest

from mcp_server.tools.admin.get_interaction_details import TOOL_NAME, handler


class TestGetInteractionDetails:
    """Tests for get_interaction_details tool."""

    def test_tool_name_no_suffix(self):
        """Verify TOOL_NAME does not end with '_tool'."""
        assert not TOOL_NAME.endswith("_tool")
        assert TOOL_NAME == "get_interaction_details"

    @pytest.mark.asyncio
    async def test_get_interaction_details_found(self):
        """Test get_interaction_details returns details with feedback."""
        mock_interaction = {"id": "int-1", "user_nlq_text": "query"}
        mock_feedback = [{"id": "fb-1", "thumb": "UP"}]

        with (
            patch(
                "mcp_server.tools.admin.get_interaction_details.get_interaction_store"
            ) as mock_get_i_store,
            patch(
                "mcp_server.tools.admin.get_interaction_details.get_feedback_store"
            ) as mock_get_f_store,
        ):

            mock_i_store = AsyncMock()
            mock_i_store.get_interaction_detail = AsyncMock(return_value=mock_interaction)
            mock_get_i_store.return_value = mock_i_store

            mock_f_store = AsyncMock()
            mock_f_store.get_feedback_for_interaction = AsyncMock(return_value=mock_feedback)
            mock_get_f_store.return_value = mock_f_store

            result = await handler("int-1")

            mock_i_store.get_interaction_detail.assert_called_once_with("int-1")
            mock_f_store.get_feedback_for_interaction.assert_called_once_with("int-1")
            assert result["id"] == "int-1"
            assert result["feedback"] == mock_feedback

    @pytest.mark.asyncio
    async def test_get_interaction_details_not_found(self):
        """Test get_interaction_details returns error when not found."""
        with (
            patch(
                "mcp_server.tools.admin.get_interaction_details.get_interaction_store"
            ) as mock_get_i_store,
            patch("mcp_server.tools.admin.get_interaction_details.get_feedback_store"),
        ):

            mock_i_store = AsyncMock()
            mock_i_store.get_interaction_detail = AsyncMock(return_value=None)
            mock_get_i_store.return_value = mock_i_store

            result = await handler("nonexistent")

            assert "error" in result
            assert "not found" in result["error"]
