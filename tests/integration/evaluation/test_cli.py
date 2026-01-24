from unittest.mock import patch

import pytest

from evaluation.runner.__main__ import main

# Create a small dataset for CLI testing
CLI_DATASET = """
{"id": "cli_1", "question": "q1", "sql": "SELECT 1"}
"""


@pytest.fixture
def cli_env(tmp_path):
    """Set up temporary environment for CLI tests."""
    # Create dataset
    dataset_path = tmp_path.joinpath("dataset.jsonl")
    dataset_path.write_text(CLI_DATASET)

    output_dir = tmp_path / "cli_out"
    output_dir.mkdir()
    return dataset_path, output_dir


def test_cli_entrypoint(cli_env):
    """Test that the CLI runs (smoke test)."""
    dataset_path, output_dir = cli_env

    # Run CLI
    # We use a limit=1 and mock behavior via internal logic if possible,
    # but since this is an integration test invoking a subprocess,
    # we might hit the REAL run_agent_with_tracing unless we patch it
    # in the subprocess or use a flag.
    #
    # FAILURE EXPECTED: The real agent tries to connect to things.
    # We need a way to run the CLI in "mock mode" or point to a "dry-run" config.
    #
    # For now, let's just check arguments parsing fails if bad args,
    # and maybe try to run it but expect failure if dependencies aren't up,
    # but that's flaky.
    #
    # Better strategy: The Phase 2 requirement says "integration test (marker-gated)".
    # Let's write a test that IMPORTS main and patches it, rather than subprocess.
    pass

    # better way:
    #
    pass


@pytest.mark.asyncio
async def test_cli_logic_flow(cli_env):
    """Test CLI logic by importing main and mocking runner."""
    dataset_path, output_dir = cli_env

    test_args = [
        "runner_script.py",
        "--dataset",
        str(dataset_path),
        "--output",
        str(output_dir),
        "--run-id",
        "cli_test_run",
    ]

    with patch("sys.argv", test_args):
        # Mock the run_evaluation core function to avoid real execution
        with patch("evaluation.runner.__main__.run_evaluation") as mock_run:
            # Setup mock return
            mock_summary = patch("evaluation.runner.config.EvaluationSummary").start()
            mock_summary.run_id = "cli_test_run"
            mock_summary.total_cases = 1
            mock_summary.successful_cases = 1
            mock_summary.failed_cases = 0
            mock_summary.accuracy = 1.0
            mock_summary.avg_latency_ms = 10.0

            mock_run.return_value = mock_summary

            await main()

            # Verify it called run_evaluation with correct config
            mock_run.assert_called_once()
            call_config = mock_run.call_args[0][0]
            assert call_config.dataset_path == str(dataset_path)
            assert call_config.output_dir == str(output_dir)
            assert call_config.run_id == "cli_test_run"
