"""Tests for feedback tools.

These tests verify the feedback tools work correctly with the DAL layer.
"""

from unittest.mock import AsyncMock, patch

import pytest
from mcp_server.tools.feedback.submit_feedback import handler as submit_feedback


@pytest.mark.asyncio
async def test_submit_feedback_upvote():
    """Verify upvote calls create_feedback only."""
    with patch("mcp_server.tools.feedback.submit_feedback.get_feedback_store") as mock_get_store:
        mock_store = AsyncMock()
        mock_store.create_feedback = AsyncMock(return_value="fb-id")
        mock_store.ensure_review_queue = AsyncMock()
        mock_get_store.return_value = mock_store

        result = await submit_feedback(interaction_id="int-1", thumb="UP", comment="Nice")

        assert result == "OK"
        mock_store.create_feedback.assert_called_once()
        mock_store.ensure_review_queue.assert_not_called()


@pytest.mark.asyncio
async def test_submit_feedback_downvote():
    """Verify downvote calls create_feedback AND ensure_review_queue."""
    with patch("mcp_server.tools.feedback.submit_feedback.get_feedback_store") as mock_get_store:
        mock_store = AsyncMock()
        mock_store.create_feedback = AsyncMock(return_value="fb-id")
        mock_store.ensure_review_queue = AsyncMock()
        mock_get_store.return_value = mock_store

        await submit_feedback(interaction_id="int-1", thumb="DOWN", comment="Bad")

        mock_store.create_feedback.assert_called_once()
        mock_store.ensure_review_queue.assert_called_once()
