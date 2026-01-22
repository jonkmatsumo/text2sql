"""Tests for evaluation runner."""

import os
import sys
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

# Add paths for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "database" / "query-target"))


class TestResultShapeValidation:
    """Tests for result shape validation logic."""

    def test_validate_result_shape_exact_row_count_pass(self):
        """Test exact row count match."""
        from agent_core.evals.run_evaluation import validate_result_shape

        result = [{"col": 1}, {"col": 2}]
        test_case = {"expected_row_count": 2}
        is_valid, error = validate_result_shape(result, test_case)
        assert is_valid is True
        assert error is None

    def test_validate_result_shape_exact_row_count_fail(self):
        """Test exact row count mismatch."""
        from agent_core.evals.run_evaluation import validate_result_shape

        result = [{"col": 1}]
        test_case = {"expected_row_count": 5}
        is_valid, error = validate_result_shape(result, test_case)
        assert is_valid is False
        assert "Row count mismatch" in error

    def test_validate_result_shape_row_count_range_pass(self):
        """Test row count within range."""
        from agent_core.evals.run_evaluation import validate_result_shape

        result = [{"col": i} for i in range(5)]
        test_case = {"expected_row_count_min": 1, "expected_row_count_max": 10}
        is_valid, error = validate_result_shape(result, test_case)
        assert is_valid is True

    def test_validate_result_shape_row_count_below_min(self):
        """Test row count below minimum."""
        from agent_core.evals.run_evaluation import validate_result_shape

        result = []
        test_case = {"expected_row_count_min": 1}
        is_valid, error = validate_result_shape(result, test_case)
        assert is_valid is False
        assert "Too few rows" in error

    def test_validate_result_shape_row_count_above_max(self):
        """Test row count above maximum."""
        from agent_core.evals.run_evaluation import validate_result_shape

        result = [{"col": i} for i in range(20)]
        test_case = {"expected_row_count_max": 10}
        is_valid, error = validate_result_shape(result, test_case)
        assert is_valid is False
        assert "Too many rows" in error

    def test_validate_result_shape_expected_columns_pass(self):
        """Test expected columns present."""
        from agent_core.evals.run_evaluation import validate_result_shape

        result = [{"id": 1, "name": "test", "extra": "val"}]
        test_case = {"expected_columns": ["id", "name"]}
        is_valid, error = validate_result_shape(result, test_case)
        assert is_valid is True

    def test_validate_result_shape_expected_columns_missing(self):
        """Test missing expected columns."""
        from agent_core.evals.run_evaluation import validate_result_shape

        result = [{"id": 1}]
        test_case = {"expected_columns": ["id", "name"]}
        is_valid, error = validate_result_shape(result, test_case)
        assert is_valid is False
        assert "Missing columns" in error

    def test_validate_result_shape_none_result(self):
        """Test None result handling."""
        from agent_core.evals.run_evaluation import validate_result_shape

        is_valid, error = validate_result_shape(None, {})
        assert is_valid is False
        assert "No result" in error


class TestFileBasedGoldenDataset:
    """Tests for file-based golden dataset loading."""

    def test_fetch_test_cases_from_file_loads_synthetic(self):
        """Test loading synthetic golden dataset from file."""
        from agent_core.evals.run_evaluation import fetch_test_cases_from_file

        test_cases = fetch_test_cases_from_file(dataset_mode="synthetic")
        assert len(test_cases) > 0
        # Verify structure
        for tc in test_cases:
            assert "test_id" in tc
            assert "question" in tc
            assert "ground_truth_sql" in tc
            assert "category" in tc
            assert "difficulty" in tc

    def test_fetch_test_cases_from_file_filters_by_category(self):
        """Test category filtering."""
        from agent_core.evals.run_evaluation import fetch_test_cases_from_file

        test_cases = fetch_test_cases_from_file(dataset_mode="synthetic", category="basic")
        assert all(tc["category"] == "basic" for tc in test_cases)

    def test_fetch_test_cases_from_file_filters_by_difficulty(self):
        """Test difficulty filtering."""
        from agent_core.evals.run_evaluation import fetch_test_cases_from_file

        test_cases = fetch_test_cases_from_file(dataset_mode="synthetic", difficulty="easy")
        assert all(tc["difficulty"] == "easy" for tc in test_cases)


class TestValidateGoldenDatasetCLI:
    """Tests for check-only validation mode."""

    def test_validate_golden_dataset_cli_success(self):
        """Test successful validation of golden dataset."""
        from agent_core.evals.run_evaluation import validate_golden_dataset_cli

        result = validate_golden_dataset_cli(dataset_mode="synthetic")
        assert result is True

    def test_validate_golden_dataset_cli_missing_file(self):
        """Test validation fails for missing golden dataset."""
        from golden.loader import GOLDEN_DATASET_FILES

        from agent_core.evals.run_evaluation import validate_golden_dataset_cli

        if GOLDEN_DATASET_FILES["pagila"].exists():
            pytest.skip("Pagila golden dataset exists, cannot test missing file error")

        # pagila doesn't have a golden dataset file yet
        result = validate_golden_dataset_cli(dataset_mode="pagila")
        assert result is False


class TestEvaluationSuiteIntegration:
    """Integration-style tests for evaluation suite."""

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_run_evaluation_suite_golden_only_dry_run(self):
        """Test dry run mode with golden dataset."""
        from agent_core.evals.run_evaluation import run_evaluation_suite

        # Dry run should not call agent or DB
        results = await run_evaluation_suite(
            dataset_mode="synthetic",
            golden_only=True,
            dry_run=True,
        )

        # Should have loaded test cases but skipped execution
        assert results is not None
        assert results["total"] == 0  # All skipped in dry run

    @pytest.mark.asyncio
    async def test_run_evaluation_suite_db_mode_mocked(self):
        """Test DB-based evaluation with mocks."""
        with patch("asyncpg.connect", new_callable=AsyncMock) as mock_connect:
            with patch(
                "agent_core.evals.run_evaluation.run_agent_with_tracing",
                new_callable=AsyncMock,
            ) as mock_run_agent:
                from agent_core.evals.run_evaluation import run_evaluation_suite

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
                mock_conn.execute = AsyncMock()

                # Agent result
                mock_run_agent.return_value = {
                    "current_sql": "SELECT 1",
                    "query_result": [{"col": 1}],
                    "error": None,
                }

                results = await run_evaluation_suite(
                    tenant_id=1,
                    dataset_mode="synthetic",
                    golden_only=False,
                )

                assert results["total"] == 1
                assert results["passed"] == 1
                assert results["failed"] == 0


class TestDBNameParameterization:
    """Tests for database configuration."""

    @pytest.mark.asyncio
    async def test_db_name_from_env(self):
        """Test that DB_NAME env var is used."""
        with patch("asyncpg.connect", new_callable=AsyncMock) as mock_connect:
            with patch.dict(os.environ, {"DB_NAME": "custom_db_name"}):
                from agent_core.evals.run_evaluation import fetch_test_cases_from_db

                mock_conn = mock_connect.return_value
                mock_conn.fetch.return_value = []

                await fetch_test_cases_from_db()

                mock_connect.assert_called()
                _, kwargs = mock_connect.call_args
                assert kwargs.get("database") == "custom_db_name"
