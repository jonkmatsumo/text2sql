import json
from unittest.mock import patch

import pytest

from airflow_evals.runner.config import EvaluationConfig, EvaluationSummary
from airflow_evals.runner.core import EvaluationRunner

# Mock dataset content
MOCK_DATASET_CONTENT = """
{"id": "case_1", "question": "Show all users", "sql": "SELECT * FROM users"}
{"id": "case_2", "question": "Count orders", "sql": "SELECT count(*) FROM orders"}
"""


@pytest.fixture
def eval_env(tmp_path):
    """Set up temporary environment for tests."""
    dataset_path = tmp_path / "dataset.jsonl"
    dataset_path.write_text(MOCK_DATASET_CONTENT)

    output_dir = tmp_path / "artifacts"
    output_dir.mkdir()

    return {"dataset": dataset_path, "output": output_dir}


@pytest.mark.asyncio
async def test_runner_execution_flow(eval_env):
    """Test full execution flow with mocked agent."""
    config = EvaluationConfig(
        dataset_path=str(eval_env["dataset"]),
        output_dir=str(eval_env["output"]),
        run_id="test_run",
        tenant_id=1,
    )

    # Mock the agent execution
    # Case 1: Correct
    # Case 2: Incorrect (simulated)
    async def mock_agent_run(question, **kwargs):
        if "users" in question:
            return {"current_sql": "SELECT * FROM users", "error": None}
        else:
            return {"current_sql": "SELECT * FROM wrong_table", "error": None}

    with patch("airflow_evals.runner.core.run_agent_with_tracing", side_effect=mock_agent_run):
        runner = EvaluationRunner(config)
        summary = await runner.run_evaluation()

        # Verify Summary
        assert summary.total_cases == 2
        assert summary.successful_cases == 1  # Only first one matched
        assert summary.accuracy == 0.5

        # Verify Artifacts Created
        run_dir = eval_env["output"] / "test_run"
        assert run_dir.exists()
        assert (run_dir / "results.json").exists()
        assert (run_dir / "summary.json").exists()

        # Verify Results Content
        with open(run_dir / "results.json") as f:
            results = json.load(f)
            assert len(results) == 2
            assert results[0]["case_id"] == "case_1"
            assert results[0]["is_correct"] is True
            assert results[1]["case_id"] == "case_2"
            assert results[1]["is_correct"] is False


@pytest.mark.asyncio
async def test_runner_handles_system_error(eval_env):
    """Test runner handles agent crashes gracefully."""
    config = EvaluationConfig(
        dataset_path=str(eval_env["dataset"]), output_dir=str(eval_env["output"]), limit=1
    )

    with patch(
        "airflow_evals.runner.core.run_agent_with_tracing", side_effect=RuntimeError("Boom")
    ):
        runner = EvaluationRunner(config)
        summary = await runner.run_evaluation()

        assert summary.total_cases == 1
        assert summary.successful_cases == 0
        assert summary.failed_cases == 1
        assert summary.accuracy == 0.0

        # Verify error status in artifacts
        run_dir = eval_env["output"] / summary.run_id
        with open(run_dir / "results.json") as f:
            results = json.load(f)
            assert results[0]["execution_status"] == "SYSTEM_ERROR"
            assert "Boom" in results[0]["error"]


@pytest.mark.asyncio
async def test_runner_detects_regression(eval_env):
    """Test that runner checks regression and persists report."""
    from airflow_evals.runner.regression import RegressionReport

    config = EvaluationConfig(
        dataset_path=str(eval_env["dataset"]), output_dir=str(eval_env["output"])
    )

    # We define a mock agent run valid for this scope
    async def mock_agent_run(**kwargs):
        return {"current_sql": "SELECT * FROM table", "error": None}

    # Mock baseline that is BETTER than current run (causing regression)
    baseline = EvaluationSummary(
        run_id="base",
        config=config,
        total_cases=1,
        successful_cases=1,
        failed_cases=0,
        accuracy=1.0,
        avg_latency_ms=0.1,
        p95_latency_ms=0.1,
    )

    mock_report = RegressionReport(
        is_regression=True,
        details=["Fake regression"],
        curr_accuracy=0.5,
        base_accuracy=1.0,
        curr_latency=10.0,
        base_latency=1.0,
    )

    with patch("airflow_evals.runner.core.run_agent_with_tracing", side_effect=mock_agent_run):
        runner = EvaluationRunner(config)

        # Mock load_baseline and check_regression
        with patch.object(runner, "load_baseline", return_value=baseline):
            with patch("airflow_evals.runner.core.RegressionDetector") as MockDetector:
                MockDetector.return_value.check_regression.return_value = mock_report

                await runner.run_evaluation()

                # Check persistence
                report_path = eval_env["output"] / runner.run_id / "regression_report.json"
                assert report_path.exists()
                assert "Fake regression" in report_path.read_text()
