"""Unit tests for PostgresEvaluationStore."""

import json
from unittest.mock import AsyncMock, MagicMock

import pytest

from dal.postgres.evaluation_store import PostgresEvaluationStore
from schema.evaluation.models import EvaluationRun, EvaluationRunCreate


@pytest.fixture
def mock_db_client():
    """Mock database client fixture."""
    client = MagicMock()
    # Mocking transaction context manager
    client.transaction.return_value.__aenter__.return_value = client
    return client


@pytest.mark.asyncio
async def test_create_run(mock_db_client):
    """Test creating a run."""
    store = PostgresEvaluationStore(db_client=mock_db_client)

    # Mock row return
    mock_row = {"id": "run-123", "started_at": "2023-01-01T00:00:00Z", "status": "RUNNING"}
    mock_db_client.fetchrow = AsyncMock(return_value=mock_row)

    run_create = EvaluationRunCreate(
        dataset_mode="synthetic", tenant_id=2, config_snapshot={"foo": "bar"}
    )

    result = await store.create_run(run_create)

    assert result.id == "run-123"
    assert result.status == "RUNNING"
    assert result.config_snapshot == {"foo": "bar"}

    # Verify SQL call
    mock_db_client.fetchrow.assert_called_once()
    args, _ = mock_db_client.fetchrow.call_args
    assert "INSERT INTO evaluation_runs" in args[0]
    assert args[1] == "synthetic"  # dataset_mode
    assert args[4] == 2  # tenant_id
    assert args[5] == json.dumps({"foo": "bar"})  # config_json


@pytest.mark.asyncio
async def test_update_run(mock_db_client):
    """Test updating a run."""
    store = PostgresEvaluationStore(db_client=mock_db_client)
    mock_db_client.execute = AsyncMock()

    run = EvaluationRun(
        id="run-123",
        dataset_mode="synthetic",
        started_at="2023-01-01T00:00:00Z",
        status="COMPLETED",
        metrics_summary={"accuracy": 1.0},
    )

    await store.update_run(run)

    mock_db_client.execute.assert_called_once()
    args, _ = mock_db_client.execute.call_args
    assert "UPDATE evaluation_runs" in args[0]
    assert args[1] == "run-123"
    assert args[2] == "COMPLETED"
    assert args[4] == json.dumps({"accuracy": 1.0})
