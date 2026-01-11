"""Tests for submit_feedback tool."""

from unittest.mock import AsyncMock, patch

import pytest
from mcp_server.tools.feedback.submit_feedback import TOOL_NAME, handler


class TestSubmitFeedback:
    """Tests for submit_feedback tool."""

    def test_tool_name_no_suffix(self):
        """Verify TOOL_NAME does not end with '_tool'."""
        assert not TOOL_NAME.endswith("_tool")
        assert TOOL_NAME == "submit_feedback"

    @pytest.mark.asyncio
    async def test_submit_feedback_upvote(self):
        """Test upvote creates feedback only."""
        with patch(
            "mcp_server.tools.feedback.submit_feedback.get_feedback_store"
        ) as mock_get_store:
            mock_store = AsyncMock()
            mock_store.create_feedback = AsyncMock(return_value="fb-id")
            mock_store.ensure_review_queue = AsyncMock()
            mock_get_store.return_value = mock_store

            result = await handler(interaction_id="int-1", thumb="UP", comment="Nice")

            assert result == "OK"
            mock_store.create_feedback.assert_called_once_with("int-1", "UP", "Nice")
            mock_store.ensure_review_queue.assert_not_called()

    @pytest.mark.asyncio
    async def test_submit_feedback_downvote(self):
        """Test downvote creates feedback AND adds to review queue."""
        with patch(
            "mcp_server.tools.feedback.submit_feedback.get_feedback_store"
        ) as mock_get_store:
            mock_store = AsyncMock()
            mock_store.create_feedback = AsyncMock(return_value="fb-id")
            mock_store.ensure_review_queue = AsyncMock()
            mock_get_store.return_value = mock_store

            result = await handler(interaction_id="int-1", thumb="DOWN", comment="Bad")

            assert result == "OK"
            mock_store.create_feedback.assert_called_once_with("int-1", "DOWN", "Bad")
            mock_store.ensure_review_queue.assert_called_once_with("int-1")

    @pytest.mark.asyncio
    async def test_submit_feedback_no_comment(self):
        """Test feedback without comment."""
        with patch(
            "mcp_server.tools.feedback.submit_feedback.get_feedback_store"
        ) as mock_get_store:
            mock_store = AsyncMock()
            mock_store.create_feedback = AsyncMock(return_value="fb-id")
            mock_get_store.return_value = mock_store

            result = await handler(interaction_id="int-1", thumb="UP")

            assert result == "OK"
            mock_store.create_feedback.assert_called_once_with("int-1", "UP", None)
