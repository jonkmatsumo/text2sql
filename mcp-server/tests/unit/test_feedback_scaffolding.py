from unittest.mock import AsyncMock

import pytest
from fixtures.feedback_fixtures import sample_feedback_payload, sample_interaction_row


def test_interaction_row_structure():
    """Verify sample interaction row matches expected schema."""
    row = sample_interaction_row()
    assert "schema_snapshot_id" in row
    assert row["execution_status"] in ["SUCCESS", "FAILURE"]
    assert isinstance(row["tables_used"], list)


def test_feedback_payload_structure():
    """Verify sample feedback payload matches expected schema."""
    payload = sample_feedback_payload()
    assert payload["thumb"] in ["UP", "DOWN"]
    assert "interaction_id" in payload


@pytest.mark.asyncio
async def test_db_mock_harness_smoke():
    """Verify that we can create a mock DB connection that mimics asyncpg."""
    # This ensures our test harness strategy (mocking) works for upcoming phases
    mock_conn = AsyncMock()
    mock_conn.execute.return_value = "INSERT 0 1"

    # Simulate an insert
    result = await mock_conn.execute("INSERT INTO foo VALUES (1)")
    assert result == "INSERT 0 1"

    # Verify we can mock fetching
    mock_conn.fetchrow.return_value = {"id": 1, "val": "test"}
    row = await mock_conn.fetchrow("SELECT * FROM foo")
    assert row["id"] == 1
