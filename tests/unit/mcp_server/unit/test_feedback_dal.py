from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from dal.postgres import PostgresFeedbackStore


@pytest.fixture
def mock_db():
    """Create mock DB."""
    db = MagicMock()
    db.fetchval = AsyncMock()
    db.execute = AsyncMock()
    db.fetch = AsyncMock()
    return db


@pytest.fixture
def dal(mock_db):
    """Create DAL."""
    return PostgresFeedbackStore(mock_db)


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


@pytest.mark.asyncio
async def test_get_feedback_for_interaction(dal, mock_db):
    """Verify get_feedback_for_interaction returns list of dicts."""
    mock_db.fetch.return_value = [{"id": "fb-1", "thumb": "UP"}]
    results = await dal.get_feedback_for_interaction(str(uuid4()))

    assert len(results) == 1
    assert results[0]["id"] == "fb-1"
    mock_db.fetch.assert_called_once()
    assert "FROM feedback" in mock_db.fetch.call_args[0][0]


@pytest.mark.asyncio
async def test_update_review_status(dal, mock_db):
    """Verify update_review_status updates the queue."""
    interaction_id = str(uuid4())
    await dal.update_review_status(
        interaction_id=interaction_id,
        status="APPROVED",
        resolution_type="FIXED",
        corrected_sql="SELECT 1",
    )

    mock_db.execute.assert_called_once()
    sql = mock_db.execute.call_args[0][0]
    assert "UPDATE review_queue" in sql
    assert "status = $2" in sql
    assert "resolution_type = $3" in sql
