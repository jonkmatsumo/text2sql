import json
from unittest.mock import MagicMock

import pytest
from mcp_server.dal.conversation import ConversationDAL


@pytest.fixture
@pytest.fixture
def mock_db():
    """Create mock DB client."""
    return MagicMock()


@pytest.fixture
def dal(mock_db):
    """Create DAL instance."""
    return ConversationDAL(mock_db)


def test_save_state_update(dal, mock_db):
    """Test saving updates existing state."""
    state_data = {"conversation_id": "123", "turns": [], "state_version": 2}

    # Setup mock to return loaded row (simulation)?
    # Or just verify strict SQL calls.
    # DAL typically uses an upsert (INSERT ... ON CONFLICT)

    dal.save_state(
        conversation_id="123", user_id="user-1", state_json=state_data, version=2, ttl_minutes=60
    )

    mock_db.execute.assert_called()
    # Check that SQL contains UPSERT logic
    sql = mock_db.execute.call_args[0][0]
    assert "INSERT INTO conversation_states" in sql
    assert "ON CONFLICT (conversation_id) DO UPDATE" in sql


def test_load_state_found(dal, mock_db):
    """Test loading existing state."""
    mock_row = {
        "state_json": json.dumps({"conversation_id": "123", "version": 5}),
        "state_version": 5,
    }
    mock_db.fetch_one.return_value = mock_row

    result = dal.load_state("123", "user-1")
    assert result is not None
    assert result["version"] == 5


def test_load_state_not_found(dal, mock_db):
    """Test return None if not found."""
    mock_db.fetch_one.return_value = None
    result = dal.load_state("999", "user-1")
    assert result is None


def test_load_state_expired(dal, mock_db):
    """Test expiration logic (if handled in DB or DAL)."""
    # If using SQL 'WHERE expires_at > NOW()', check generated SQL
    dal.load_state("123", "user-1")
    sql = mock_db.fetch_one.call_args[0][0]
    assert "AND expires_at > NOW()" in sql
