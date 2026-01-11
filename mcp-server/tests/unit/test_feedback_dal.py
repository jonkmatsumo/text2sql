from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest
from mcp_server.dal.feedback import FeedbackDAL


@pytest.fixture
def mock_db():
    """Create mock DB."""
    db = MagicMock()
    db.fetchval = AsyncMock()
    db.execute = AsyncMock()
    return db


@pytest.fixture
def dal(mock_db):
    """Create DAL."""
    return FeedbackDAL(mock_db)


@pytest.mark.asyncio
async def test_submit_feedback_persists_row(dal, mock_db):
    """Verify create_feedback calls insert."""
    interaction_id = str(uuid4())
    # Mock returning id
    mock_db.fetchval.return_value = "fb-1"

    result_id = await dal.create_feedback(
        interaction_id=interaction_id, thumb="UP", comment="Great!", feedback_source="user"
    )

    assert result_id == "fb-1"
    mock_db.fetchval.assert_called_once()
    sql = mock_db.fetchval.call_args[0][0]
    assert "INSERT INTO feedback" in sql
    assert "thumb" in sql


@pytest.mark.asyncio
async def test_downvote_creates_review_queue_item_idempotent(dal, mock_db):
    """Verify ensure_review_queue inserts unique pending item."""
    interaction_id = str(uuid4())

    await dal.ensure_review_queue(interaction_id)

    mock_db.execute.assert_called_once()
    sql = mock_db.execute.call_args[0][0]
    assert "INSERT INTO review_queue" in sql
    assert "ON CONFLICT" in sql  # ensuring idempotency
    assert "status = 'PENDING'" in sql
