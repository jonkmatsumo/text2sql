import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from airflow_evals.runner.config import EvaluationConfig
from airflow_evals.runner.core import EvaluationRunner, EvaluationSummary

# Mock Schema
SCHEMA_PATH = Path("airflow_evals/schema/metrics_v1.json")


def test_metrics_schema_valid():
    """Ensure the schema file is valid JSON."""
    assert SCHEMA_PATH.exists()
    schema = json.loads(SCHEMA_PATH.read_text())
    assert schema["title"] == "EvaluationMetricsV1"
    assert "exact_match_rate" in schema["required"]


@pytest.mark.asyncio
async def test_mlflow_logging():
    """Test that log_to_mlflow calls mlflow methods correctly."""
    config = EvaluationConfig(
        dataset_path="dummy.jsonl",
        output_dir="dummy_out",
        tenant_id=1,
        git_sha="abc1234",
        run_id="test_run",
    )
    runner = EvaluationRunner(config)

    # Mock Summary
    summary = EvaluationSummary(
        run_id="test_run",
        config=config,
        total_cases=10,
        successful_cases=9,
        failed_cases=1,
        accuracy=0.9,
        avg_latency_ms=100.0,
        p95_latency_ms=150.0,
    )

    # Mock Results
    results = []  # we don't inspect results in detail for this test

    # Mock mlflow
    with patch("airflow_evals.runner.core.MLFLOW_AVAILABLE", True):
        with patch("airflow_evals.runner.core.mlflow") as mock_mlflow:
            mock_run = MagicMock()
            mock_mlflow.start_run.return_value.__enter__.return_value = mock_run

            runner.log_to_mlflow(summary, results)

            # Checks
            mock_mlflow.set_tracking_uri.assert_called()
            mock_mlflow.set_experiment.assert_called()
            mock_mlflow.start_run.assert_called_with(run_name="test_run")

            # Params
            mock_mlflow.log_params.assert_called()
            call_args = mock_mlflow.log_params.call_args[0][0]
            assert call_args["dataset"] == "dummy.jsonl"

            mock_mlflow.log_param.assert_called_with("git_sha", "abc1234")

            # Metrics
            mock_mlflow.log_metrics.assert_called()
            metrics_args = mock_mlflow.log_metrics.call_args[0][0]
            assert metrics_args["accuracy"] == 0.9


@pytest.mark.asyncio
async def test_mlflow_graceful_missing():
    """Test that runner doesn't crash if MLflow is missing."""
    config = EvaluationConfig(dataset_path="d", output_dir="o")
    runner = EvaluationRunner(config)
    summary = MagicMock()

    with patch("airflow_evals.runner.core.MLFLOW_AVAILABLE", False):
        # Should not raise
        runner.log_to_mlflow(summary, [])
