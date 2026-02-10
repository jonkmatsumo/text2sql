import json
from unittest.mock import AsyncMock, MagicMock

import pytest

from dal.postgres import PostgresConversationStore


@pytest.fixture
def mock_db():
    """Create mock DB client."""
    db = MagicMock()
    db.execute = AsyncMock()
    db.fetchrow = AsyncMock()
    db.fetchval = AsyncMock(return_value=None)
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
        conversation_id="123",
        user_id="user-1",
        tenant_id=7,
        state_json=state_data,
        version=2,
        ttl_minutes=60,
    )

    mock_db.execute.assert_called_once()
    # Check SQL args
    args = mock_db.execute.call_args[0]
    assert "INSERT INTO conversation_states" in args[0]
    assert args[1] == "123"  # conversation_id
    assert args[2] == "user-1"  # user_id
    assert args[3] == 7  # tenant_id


@pytest.mark.asyncio
async def test_load_state_found(dal, mock_db):
    """Test loading existing state."""
    mock_row = {
        "state_json": json.dumps({"conversation_id": "123", "version": 5}),
        "state_version": 5,
    }
    mock_db.fetchrow.return_value = mock_row
    mock_db.fetchval.return_value = 7

    result = await dal.load_state_async("123", "user-1", 7)
    assert result is not None
    assert result["version"] == 5


@pytest.mark.asyncio
async def test_load_state_not_found(dal, mock_db):
    """Test return None if not found."""
    mock_db.fetchrow.return_value = None
    mock_db.fetchval.return_value = None
    result = await dal.load_state_async("999", "user-1", 1)
    assert result is None


@pytest.mark.asyncio
async def test_load_state_cross_tenant_rejected(dal, mock_db):
    """Tenant mismatch on read should raise deterministic error."""
    mock_db.fetchval.return_value = 2
    with pytest.raises(ValueError, match="Tenant mismatch"):
        await dal.load_state_async("123", "user-1", 1)


@pytest.mark.asyncio
async def test_save_state_cross_tenant_rejected(dal, mock_db):
    """Tenant mismatch on existing conversation_id should be rejected."""
    mock_db.fetchval.return_value = 2
    with pytest.raises(ValueError, match="Tenant mismatch"):
        await dal.save_state_async(
            conversation_id="123",
            user_id="user-1",
            tenant_id=1,
            state_json={"conversation_id": "123", "turns": []},
            version=1,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["load", "save"])
async def test_conversation_scope_violation_message_consistent(dal, mock_db, operation):
    """Read/write tenant mismatches should use a consistent deterministic error message."""
    mock_db.fetchval.return_value = 7

    with pytest.raises(ValueError, match="Tenant mismatch for conversation scope."):
        if operation == "load":
            await dal.load_state_async("123", "user-1", 1)
        else:
            await dal.save_state_async(
                conversation_id="123",
                user_id="user-1",
                tenant_id=1,
                state_json={"conversation_id": "123"},
                version=1,
            )
