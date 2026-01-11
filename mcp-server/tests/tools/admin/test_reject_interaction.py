"""Tests for reject_interaction tool."""

from unittest.mock import AsyncMock, patch

import pytest
from mcp_server.tools.admin.reject_interaction import TOOL_NAME, handler


class TestRejectInteraction:
    """Tests for reject_interaction tool."""

    def test_tool_name_no_suffix(self):
        """Verify TOOL_NAME does not end with '_tool'."""
        assert not TOOL_NAME.endswith("_tool")
        assert TOOL_NAME == "reject_interaction"

    @pytest.mark.asyncio
    async def test_reject_interaction_success(self):
        """Test reject_interaction updates status."""
        with patch(
            "mcp_server.tools.admin.reject_interaction.get_feedback_store"
        ) as mock_get_store:
            mock_store = AsyncMock()
            mock_store.update_review_status = AsyncMock()
            mock_get_store.return_value = mock_store

            result = await handler(
                interaction_id="int-1", reason="BAD_QUERY", reviewer_notes="Cannot parse"
            )

            assert result == "OK"
            mock_store.update_review_status.assert_called_once()
            call_kwargs = mock_store.update_review_status.call_args[1]
            assert call_kwargs["interaction_id"] == "int-1"
            assert call_kwargs["status"] == "REJECTED"
            assert call_kwargs["resolution_type"] == "BAD_QUERY"
            assert call_kwargs["reviewer_notes"] == "Cannot parse"

    @pytest.mark.asyncio
    async def test_reject_interaction_default_reason(self):
        """Test default rejection reason."""
        with patch(
            "mcp_server.tools.admin.reject_interaction.get_feedback_store"
        ) as mock_get_store:
            mock_store = AsyncMock()
            mock_store.update_review_status = AsyncMock()
            mock_get_store.return_value = mock_store

            await handler(interaction_id="int-1")

            call_kwargs = mock_store.update_review_status.call_args[1]
            assert call_kwargs["resolution_type"] == "CANNOT_FIX"
