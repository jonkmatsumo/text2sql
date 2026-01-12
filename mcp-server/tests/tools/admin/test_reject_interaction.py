"""Tests for reject_interaction tool."""

from unittest.mock import MagicMock, patch

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
            calls = {}

            async def update_review_status(**kwargs):
                calls.update(kwargs)

            mock_store = MagicMock()
            mock_store.update_review_status = update_review_status
            mock_get_store.return_value = mock_store

            result = await handler(
                interaction_id="int-1", reason="BAD_QUERY", reviewer_notes="Cannot parse"
            )

            assert result == "OK"
            assert calls["interaction_id"] == "int-1"
            assert calls["status"] == "REJECTED"
            assert calls["resolution_type"] == "BAD_QUERY"
            assert calls["reviewer_notes"] == "Cannot parse"

    @pytest.mark.asyncio
    async def test_reject_interaction_default_reason(self):
        """Test default rejection reason."""
        with patch(
            "mcp_server.tools.admin.reject_interaction.get_feedback_store"
        ) as mock_get_store:
            calls = {}

            async def update_review_status(**kwargs):
                calls.update(kwargs)

            mock_store = MagicMock()
            mock_store.update_review_status = update_review_status
            mock_get_store.return_value = mock_store

            await handler(interaction_id="int-1")

            assert calls["resolution_type"] == "CANNOT_FIX"
