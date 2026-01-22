"""Integration tests for Evaluation Store."""

import os

import pytest

from dal.database import Database
from dal.factory import get_evaluation_store
from schema.evaluation.models import EvaluationCaseResultCreate, EvaluationRunCreate

# Skip if no DB creds (simple check)
pytestmark = pytest.mark.skipif(
    not os.getenv("DB_HOST"), reason="Integration test requires DB connection"
)


@pytest.mark.asyncio
async def test_evaluation_round_trip():
    """Test full round trip: create run, add results, update run, read back."""
    # 1. Init DB
    try:
        await Database.init()
    except Exception as e:
        pytest.skip(f"Failed to connect to DB: {e}")

    store = get_evaluation_store()

    # 2. Create Run
    run_create = EvaluationRunCreate(
        dataset_mode="synthetic",
        dataset_version="v1-test",
        git_sha="abcdef",
        tenant_id=1,
        config_snapshot={"test": "integration"},
    )

    run = await store.create_run(run_create)
    assert run.id is not None
    assert run.status == "RUNNING"

    # 3. Add Results
    results = [
        EvaluationCaseResultCreate(
            run_id=run.id,
            test_id="case-1",
            question="SELECT 1",
            generated_sql="SELECT 1",
            is_correct=True,
            structural_score=1.0,
            execution_time_ms=10,
            raw_response={"foo": "bar"},
        ),
        EvaluationCaseResultCreate(
            run_id=run.id,
            test_id="case-2",
            question="SELECT 2",
            generated_sql="SELECT 3",
            is_correct=False,
            structural_score=0.5,
            error_message="Wrong number",
            execution_time_ms=20,
        ),
    ]

    await store.save_case_results(results)

    # 4. Update Run
    run.status = "COMPLETED"
    run.metrics_summary = {"accuracy": 0.5}
    await store.update_run(run)

    # 5. Read Back
    fetched = await store.get_run(run.id)
    assert fetched is not None
    assert fetched.dataset_mode == "synthetic"
    assert fetched.status == "COMPLETED"
    assert fetched.metrics_summary == {"accuracy": 0.5}

    # Cleanup (Optional)
    # We rely on test DB cleanup or transaction rollback if configured,
    # but here we are just validating it works.

    await Database.close()
