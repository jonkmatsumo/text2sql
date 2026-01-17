"""Tests for evaluation runner."""

import os
from unittest.mock import AsyncMock, patch

import pytest

# sys.modules patching removed as it was unused/unnecessary given tests passed


@pytest.mark.asyncio
async def test_run_evaluation_suite_flow():
    """Test the orchestration of evaluation suite."""
    # We need to import the module. Since it is a script, we might need
    # to load it dynamically.
    # We'll try to import functions assuming deps are installed.

    # Mocking asyncpg and agent_core.graph
    with patch("asyncpg.connect", new_callable=AsyncMock) as mock_connect:
        with patch(
            "agent.scripts.run_evaluation.run_agent_with_tracing",
            new_callable=AsyncMock,
        ) as mock_run_agent:
            from agent.scripts.run_evaluation import run_evaluation_suite

            # Setup mocks
            mock_conn = mock_connect.return_value
            mock_conn.fetch.return_value = [
                {
                    "test_id": 1,
                    "question": "test q",
                    "ground_truth_sql": "SELECT 1",
                    "expected_row_count": 1,
                    "category": "test",
                    "difficulty": "easy",
                    "tenant_id": 1,
                }
            ]

            # Agent result
            mock_run_agent.return_value = {
                "current_sql": "SELECT 1",
                "query_result": [{"col": 1}],
                "error": None,
            }

            # Ground truth execution
            # fetch called again for fetch_test_cases (first) and execute_ground_truth_sql (second)?
            # invoke fetch side effect?
            # fetch_test_cases calls conn.fetch.
            # execute_ground_truth_sql calls conn.fetch logic.
            # store_evaluation_result calls conn.execute.

            # We need precise control over mock returned values to handle different calls
            # But simplest is to verify flow runs without exception.

            results = await run_evaluation_suite(tenant_id=1)

            assert results["total"] == 1
            assert results["passed"] == 1
            assert results["failed"] == 0


@pytest.mark.asyncio
async def test_db_name_parameterization():
    """Test that DB_NAME env var is used."""
    with patch("asyncpg.connect", new_callable=AsyncMock) as mock_connect:
        with patch.dict(os.environ, {"DB_NAME": "synthetic_db"}):
            from agent.scripts.run_evaluation import fetch_test_cases

            mock_conn = mock_connect.return_value
            mock_conn.fetch.return_value = []

            await fetch_test_cases()

            # Check connection args
            mock_connect.assert_called()
            args, kwargs = mock_connect.call_args
            assert kwargs.get("database") == "synthetic_db"
