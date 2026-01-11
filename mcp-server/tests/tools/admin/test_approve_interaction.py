"""Tests for approve_interaction tool."""

from unittest.mock import AsyncMock, patch

import pytest
from mcp_server.tools.admin.approve_interaction import TOOL_NAME, handler


class TestApproveInteraction:
    """Tests for approve_interaction tool."""

    def test_tool_name_no_suffix(self):
        """Verify TOOL_NAME does not end with '_tool'."""
        assert not TOOL_NAME.endswith("_tool")
        assert TOOL_NAME == "approve_interaction"

    @pytest.mark.asyncio
    async def test_approve_interaction_success(self):
        """Test approve_interaction updates status."""
        with patch(
            "mcp_server.tools.admin.approve_interaction.get_feedback_store"
        ) as mock_get_store:
            mock_store = AsyncMock()
            mock_store.update_review_status = AsyncMock()
            mock_get_store.return_value = mock_store

            result = await handler(
                interaction_id="int-1", corrected_sql="SELECT 1", resolution_type="FIXED"
            )

            assert result == "OK"
            mock_store.update_review_status.assert_called_once()
            call_kwargs = mock_store.update_review_status.call_args[1]
            assert call_kwargs["interaction_id"] == "int-1"
            assert call_kwargs["status"] == "APPROVED"
            assert call_kwargs["resolution_type"] == "FIXED"
            assert call_kwargs["corrected_sql"] == "SELECT 1"

    @pytest.mark.asyncio
    async def test_approve_interaction_default_resolution(self):
        """Test default resolution type."""
        with patch(
            "mcp_server.tools.admin.approve_interaction.get_feedback_store"
        ) as mock_get_store:
            mock_store = AsyncMock()
            mock_store.update_review_status = AsyncMock()
            mock_get_store.return_value = mock_store

            await handler(interaction_id="int-1")

            call_kwargs = mock_store.update_review_status.call_args[1]
            assert call_kwargs["resolution_type"] == "APPROVED_AS_IS"
