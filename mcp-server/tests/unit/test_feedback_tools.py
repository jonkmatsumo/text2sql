from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from mcp_server.tools.feedback_tools import submit_feedback_tool


@pytest.fixture
def mock_db_connection():
    """Create mock DB connection."""
    cm = AsyncMock()
    cm.__aenter__.return_value = MagicMock()
    cm.__aexit__.return_value = None
    return cm


@patch("mcp_server.config.database.Database.get_connection")
@patch("mcp_server.tools.feedback_tools.FeedbackDAL")
@pytest.mark.asyncio
async def test_submit_feedback_tool_upvote(MockDAL, mock_get_conn, mock_db_connection):
    """Verify upvote calls create_feedback only."""
    mock_get_conn.return_value = mock_db_connection
    dal = MockDAL.return_value
    dal.create_feedback = AsyncMock(return_value="fb-id")

    result = await submit_feedback_tool(interaction_id="int-1", thumb="UP", comment="Nice")

    assert result == "OK"
    dal.create_feedback.assert_called_once()
    dal.ensure_review_queue.assert_not_called()


@patch("mcp_server.config.database.Database.get_connection")
@patch("mcp_server.tools.feedback_tools.FeedbackDAL")
@pytest.mark.asyncio
async def test_submit_feedback_tool_downvote(MockDAL, mock_get_conn, mock_db_connection):
    """Verify downvote calls create_feedback AND ensure_review_queue."""
    mock_get_conn.return_value = mock_db_connection
    dal = MockDAL.return_value
    dal.create_feedback = AsyncMock(return_value="fb-id")
    dal.ensure_review_queue = AsyncMock()

    await submit_feedback_tool(interaction_id="int-1", thumb="DOWN", comment="Bad")

    dal.create_feedback.assert_called_once()
    dal.ensure_review_queue.assert_called_once()
