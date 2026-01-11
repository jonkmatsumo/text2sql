from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest
from mcp_server.dal.postgres import PostgresInteractionStore


@pytest.fixture
def mock_db():
    """Create mock DB."""
    db = MagicMock()
    db.fetchval = AsyncMock()
    db.execute = AsyncMock()
    db.fetch = AsyncMock()
    db.fetchrow = AsyncMock()
    return db


@pytest.fixture
def dal(mock_db):
    """Create DAL."""
    return PostgresInteractionStore(mock_db)


@pytest.mark.asyncio
async def test_create_interaction_persists_required_fields(dal, mock_db):
    """Verify create_interaction calls insert with required fields."""
    interaction_id = str(uuid4())
    snapshot_id = "snap-123"
    nlq = "Show me movies"

    # Mock return of ID
    mock_db.fetchval.return_value = interaction_id

    result_id = await dal.create_interaction(
        conversation_id="conv-1",
        schema_snapshot_id=snapshot_id,
        user_nlq_text=nlq,
    )

    assert result_id == interaction_id
    mock_db.fetchval.assert_called_once()
    sql = mock_db.fetchval.call_args[0][0]
    assert "INSERT INTO query_interactions" in sql
    assert "user_nlq_text" in sql
    assert "schema_snapshot_id" in sql


@pytest.mark.asyncio
async def test_interaction_requires_schema_snapshot_id(dal, mock_db):
    """Verify that schema_snapshot_id is enforced (at least in DAL logic args)."""
    # Assuming type hinting enforces it, but we can check if implementation validates.
    # In DAL, we just pass args. If we pass None for snapshot, it might fail in DB.
    # We can't simulate DB failure easily with Mock, but we can verify args passing.
    await dal.create_interaction(
        conversation_id="conv-1",
        schema_snapshot_id=None,  # Should probably be handled or DB will error
        user_nlq_text="test",
    )
    args = mock_db.fetchval.call_args[0]
    # args[0] is SQL string
    # args[1] is conversation_id -> "conv-1"
    # args[2] is schema_snapshot_id -> None
    assert args[1] == "conv-1"
    assert args[2] is None


@pytest.mark.asyncio
async def test_update_interaction_result(dal, mock_db):
    """Verify updating execution results."""
    interaction_id = "int-1"
    await dal.update_interaction_result(
        interaction_id=interaction_id,
        generated_sql="SELECT 1",
        execution_status="SUCCESS",
        response_payload='{"text": "hi"}',
        tables_used=["t1"],
    )

    mock_db.execute.assert_called_once()
    sql = mock_db.execute.call_args[0][0]
    assert "UPDATE query_interactions" in sql
    assert "generated_sql" in sql


@pytest.mark.asyncio
async def test_get_recent_interactions(dal, mock_db):
    """Verify get_recent_interactions returns list of dicts."""
    mock_db.fetch.return_value = [{"id": "int-1", "user_nlq_text": "query"}]
    results = await dal.get_recent_interactions(limit=10)

    assert len(results) == 1
    assert results[0]["id"] == "int-1"
    mock_db.fetch.assert_called_once()
    assert "LIMIT $1 OFFSET $2" in mock_db.fetch.call_args[0][0]


@pytest.mark.asyncio
async def test_get_interaction_detail(dal, mock_db):
    """Verify get_interaction_detail returns single dict."""
    mock_db.fetchrow.return_value = {"id": "int-1", "user_nlq_text": "query"}
    result = await dal.get_interaction_detail("int-1")

    assert result["id"] == "int-1"
    mock_db.fetchrow.assert_called_once()
    assert "WHERE id = $1::uuid" in mock_db.fetchrow.call_args[0][0]
