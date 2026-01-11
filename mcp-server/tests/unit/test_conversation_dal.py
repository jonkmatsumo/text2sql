import json
from unittest.mock import AsyncMock, MagicMock

import pytest
from mcp_server.dal.postgres import PostgresConversationStore


@pytest.fixture
def mock_db():
    """Create mock DB client."""
    db = MagicMock()
    db.execute = AsyncMock()
    db.fetchrow = AsyncMock()
    return db


@pytest.fixture
def dal(mock_db):
    """Create DAL instance."""
    return PostgresConversationStore(mock_db)


@pytest.mark.asyncio
async def test_save_state_update(dal, mock_db):
    """Test saving updates existing state."""
    state_data = {"conversation_id": "123", "turns": [], "state_version": 2}

    await dal.save_state_async(
        conversation_id="123", user_id="user-1", state_json=state_data, version=2, ttl_minutes=60
    )

    mock_db.execute.assert_called_once()
    # Check SQL args
    args = mock_db.execute.call_args[0]
    assert "INSERT INTO conversation_states" in args[0]
    assert args[1] == "123"  # conversation_id
    assert args[2] == "user-1"  # user_id


@pytest.mark.asyncio
async def test_load_state_found(dal, mock_db):
    """Test loading existing state."""
    mock_row = {
        "state_json": json.dumps({"conversation_id": "123", "version": 5}),
        "state_version": 5,
    }
    mock_db.fetchrow.return_value = mock_row

    result = await dal.load_state_async("123", "user-1")
    assert result is not None
    assert result["version"] == 5


@pytest.mark.asyncio
async def test_load_state_not_found(dal, mock_db):
    """Test return None if not found."""
    mock_db.fetchrow.return_value = None
    result = await dal.load_state_async("999", "user-1")
    assert result is None
