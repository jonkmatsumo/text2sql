from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest
from mcp_server.dal.postgres.pattern_run_store import PostgresPatternRunStore


@pytest.fixture
def mock_db():
    """Create a mock database client."""
    db = MagicMock()
    db.fetchrow = AsyncMock()
    db.fetch = AsyncMock()
    db.execute = AsyncMock()
    db.executemany = AsyncMock()
    return db


@pytest.fixture
def store(mock_db):
    """Create a store with mock DB."""
    return PostgresPatternRunStore(mock_db)


@pytest.mark.asyncio
async def test_create_run(store, mock_db):
    """Test creating a run."""
    run_id = uuid4()
    mock_db.fetchrow.return_value = {"id": run_id}

    result = await store.create_run(
        status="RUNNING", target_table="table", config_snapshot={"k": "v"}
    )

    assert result == run_id
    mock_db.fetchrow.assert_called_once()
    sql = mock_db.fetchrow.call_args[0][0]
    assert "INSERT INTO nlp_pattern_runs" in sql
    assert "status" in sql


@pytest.mark.asyncio
async def test_add_items(store, mock_db):
    """Test adding items to a run."""
    run_id = uuid4()
    items = [{"pattern_id": "P1", "label": "L", "pattern": "p", "action": "CREATED"}]

    await store.add_run_items(run_id, items)

    mock_db.executemany.assert_called_once()
    sql = mock_db.executemany.call_args[0][0]
    assert "INSERT INTO nlp_pattern_run_items" in sql


@pytest.mark.asyncio
async def test_update_run(store, mock_db):
    """Test updating a run."""
    run_id = uuid4()
    await store.update_run(run_id, status="COMPLETED")

    mock_db.execute.assert_called_once()
    sql = mock_db.execute.call_args[0][0]
    assert "UPDATE nlp_pattern_runs" in sql
    assert "completed_at" in sql
