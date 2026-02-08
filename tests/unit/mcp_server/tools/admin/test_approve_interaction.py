"""Tests for approve_interaction tool."""

import json
from unittest.mock import MagicMock, patch

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
            calls = {}

            async def update_review_status(**kwargs):
                calls.update(kwargs)

            mock_store = MagicMock()
            mock_store.update_review_status = update_review_status
            mock_get_store.return_value = mock_store

            result = await handler(
                interaction_id="int-1", corrected_sql="SELECT 1", resolution_type="FIXED"
            )

            data = json.loads(result)
            assert data["result"]["status"] == "OK"
            assert calls["interaction_id"] == "int-1"
            assert calls["status"] == "APPROVED"
            assert calls["resolution_type"] == "FIXED"
            assert calls["corrected_sql"] == "SELECT 1"

    @pytest.mark.asyncio
    async def test_approve_interaction_default_resolution(self):
        """Test default resolution type."""
        with patch(
            "mcp_server.tools.admin.approve_interaction.get_feedback_store"
        ) as mock_get_store:
            calls = {}

            async def update_review_status(**kwargs):
                calls.update(kwargs)

            mock_store = MagicMock()
            mock_store.update_review_status = update_review_status
            mock_get_store.return_value = mock_store

            await handler(interaction_id="int-1")

            assert calls["resolution_type"] == "APPROVED_AS_IS"
