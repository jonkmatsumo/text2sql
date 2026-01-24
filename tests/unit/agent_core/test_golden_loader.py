"""Tests for golden dataset loader and schema validation."""

import json

# Add database/query-target/golden to path for imports
import sys
import tempfile
from pathlib import Path

import pytest

_GOLDEN_PKG_PATH = (
    Path(__file__).resolve().parents[3] / "data" / "database" / "query-target" / "golden"
)
sys.path.insert(0, str(_GOLDEN_PKG_PATH.parent))

from golden import (  # noqa: E402
    GoldenDataset,
    GoldenDatasetNotFoundError,
    GoldenDatasetValidationError,
    GoldenTestCase,
    load_golden_dataset,
    load_test_cases,
    validate_golden_dataset,
)


class TestGoldenSchema:
    """Tests for GoldenTestCase and GoldenDataset dataclasses."""

    def test_golden_test_case_from_dict_required_fields(self):
        """Test creating GoldenTestCase with required fields only."""
        data = {
            "id": "test-1",
            "nlq": "How many customers?",
            "expected_sql": "SELECT COUNT(*) FROM customers",
            "category": "basic",
            "difficulty": "easy",
        }
        tc = GoldenTestCase.from_dict(data)
        assert tc.id == "test-1"
        assert tc.nlq == "How many customers?"
        assert tc.expected_sql == "SELECT COUNT(*) FROM customers"
        assert tc.category == "basic"
        assert tc.difficulty == "easy"
        assert tc.expected_columns == []
        assert tc.expected_row_count is None
        assert tc.skip is False

    def test_golden_test_case_from_dict_all_fields(self):
        """Test creating GoldenTestCase with all optional fields."""
        data = {
            "id": "test-2",
            "nlq": "How many customers?",
            "expected_sql": "SELECT COUNT(*) FROM customers",
            "category": "aggregation",
            "difficulty": "medium",
            "expected_columns": ["count"],
            "expected_row_count": 1,
            "intent": "count_entities",
            "notes": "Test note",
            "skip": True,
            "skip_reason": "Not yet implemented",
        }
        tc = GoldenTestCase.from_dict(data)
        assert tc.expected_columns == ["count"]
        assert tc.expected_row_count == 1
        assert tc.intent == "count_entities"
        assert tc.notes == "Test note"
        assert tc.skip is True
        assert tc.skip_reason == "Not yet implemented"

    def test_golden_dataset_from_dict(self):
        """Test creating GoldenDataset with test cases."""
        data = {
            "version": "1.0",
            "dataset_mode": "synthetic",
            "test_cases": [
                {
                    "id": "test-1",
                    "nlq": "Query one",
                    "expected_sql": "SELECT 1",
                    "category": "basic",
                    "difficulty": "easy",
                },
                {
                    "id": "test-2",
                    "nlq": "Query two",
                    "expected_sql": "SELECT 2",
                    "category": "aggregation",
                    "difficulty": "medium",
                    "skip": True,
                },
            ],
        }
        dataset = GoldenDataset.from_dict(data)
        assert dataset.version == "1.0"
        assert dataset.dataset_mode == "synthetic"
        assert len(dataset.test_cases) == 2

    def test_golden_dataset_filter_by_category(self):
        """Test filtering test cases by category."""
        data = {
            "version": "1.0",
            "dataset_mode": "synthetic",
            "test_cases": [
                {
                    "id": "t1",
                    "nlq": "Q1",
                    "expected_sql": "S1",
                    "category": "basic",
                    "difficulty": "easy",
                },
                {
                    "id": "t2",
                    "nlq": "Q2",
                    "expected_sql": "S2",
                    "category": "aggregation",
                    "difficulty": "easy",
                },
                {
                    "id": "t3",
                    "nlq": "Q3",
                    "expected_sql": "S3",
                    "category": "basic",
                    "difficulty": "medium",
                },
            ],
        }
        dataset = GoldenDataset.from_dict(data)
        basic_cases = dataset.get_by_category("basic")
        assert len(basic_cases) == 2

    def test_golden_dataset_get_active(self):
        """Test filtering out skipped test cases."""
        data = {
            "version": "1.0",
            "dataset_mode": "synthetic",
            "test_cases": [
                {
                    "id": "t1",
                    "nlq": "Q1",
                    "expected_sql": "S1",
                    "category": "basic",
                    "difficulty": "easy",
                },
                {
                    "id": "t2",
                    "nlq": "Q2",
                    "expected_sql": "S2",
                    "category": "basic",
                    "difficulty": "easy",
                    "skip": True,
                },
            ],
        }
        dataset = GoldenDataset.from_dict(data)
        active = dataset.get_active()
        assert len(active) == 1
        assert active[0].id == "t1"


class TestValidation:
    """Tests for schema validation."""

    def test_validate_valid_dataset(self):
        """Test validation of a valid dataset."""
        data = {
            "version": "1.0",
            "dataset_mode": "synthetic",
            "test_cases": [
                {
                    "id": "test-1",
                    "nlq": "How many customers?",
                    "expected_sql": "SELECT COUNT(*) FROM customers",
                    "category": "basic",
                    "difficulty": "easy",
                }
            ],
        }
        errors = validate_golden_dataset(data)
        assert errors == []

    def test_validate_missing_version(self):
        """Test validation catches missing version."""
        data = {
            "dataset_mode": "synthetic",
            "test_cases": [
                {
                    "id": "t1",
                    "nlq": "Query",
                    "expected_sql": "SELECT 1",
                    "category": "basic",
                    "difficulty": "easy",
                }
            ],
        }
        errors = validate_golden_dataset(data)
        assert any("version" in e for e in errors)

    def test_validate_empty_test_cases(self):
        """Test validation catches empty test_cases array."""
        data = {"version": "1.0", "dataset_mode": "synthetic", "test_cases": []}
        errors = validate_golden_dataset(data)
        assert any("at least one" in e for e in errors)

    def test_validate_invalid_category(self):
        """Test validation catches invalid category."""
        data = {
            "version": "1.0",
            "dataset_mode": "synthetic",
            "test_cases": [
                {
                    "id": "t1",
                    "nlq": "Query",
                    "expected_sql": "SELECT 1",
                    "category": "invalid_category",
                    "difficulty": "easy",
                }
            ],
        }
        errors = validate_golden_dataset(data)
        assert any("category" in e for e in errors)

    def test_validate_invalid_difficulty(self):
        """Test validation catches invalid difficulty."""
        data = {
            "version": "1.0",
            "dataset_mode": "synthetic",
            "test_cases": [
                {
                    "id": "t1",
                    "nlq": "Query",
                    "expected_sql": "SELECT 1",
                    "category": "basic",
                    "difficulty": "expert",
                }
            ],
        }
        errors = validate_golden_dataset(data)
        assert any("difficulty" in e for e in errors)

    def test_validate_missing_required_test_case_field(self):
        """Test validation catches missing required fields in test case."""
        data = {
            "version": "1.0",
            "dataset_mode": "synthetic",
            "test_cases": [
                {
                    "id": "t1",
                    "nlq": "Query",
                    # missing expected_sql, category, difficulty
                }
            ],
        }
        errors = validate_golden_dataset(data)
        assert any("expected_sql" in e for e in errors)
        assert any("category" in e for e in errors)
        assert any("difficulty" in e for e in errors)


class TestLoader:
    """Tests for golden dataset loader."""

    def test_load_synthetic_golden_dataset(self):
        """Test loading the actual synthetic golden dataset."""
        dataset = load_golden_dataset("synthetic")
        assert dataset.dataset_mode == "synthetic"
        assert len(dataset.test_cases) > 0
        # Check we have diverse categories
        categories = {tc.category for tc in dataset.test_cases}
        assert len(categories) >= 3  # Should have multiple categories

    def test_load_missing_file_raises_error(self):
        """Test loading non-existent file raises clear error."""
        from golden.loader import GOLDEN_DATASET_FILES

        if GOLDEN_DATASET_FILES["pagila"].exists():
            pytest.skip("Pagila golden dataset exists, cannot test missing file error")

        with pytest.raises(GoldenDatasetNotFoundError) as exc_info:
            load_golden_dataset("pagila")  # pagila doesn't have a golden dataset yet
        assert "not found" in str(exc_info.value).lower()

    def test_load_custom_path(self):
        """Test loading from a custom path."""
        data = {
            "version": "1.0",
            "dataset_mode": "synthetic",
            "test_cases": [
                {
                    "id": "t1",
                    "nlq": "Test query",
                    "expected_sql": "SELECT 1",
                    "category": "basic",
                    "difficulty": "easy",
                }
            ],
        }
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(data, f)
            f.flush()
            dataset = load_golden_dataset("synthetic", path=f.name)
            assert len(dataset.test_cases) == 1

    def test_load_invalid_json_raises_error(self):
        """Test loading invalid JSON raises clear error."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            f.write("{ invalid json }")
            f.flush()
            with pytest.raises(GoldenDatasetValidationError) as exc_info:
                load_golden_dataset("synthetic", path=f.name)
            assert "Invalid JSON" in str(exc_info.value)

    def test_load_validation_failure_raises_error(self):
        """Test loading file that fails validation raises clear error."""
        data = {"version": "1.0", "dataset_mode": "synthetic", "test_cases": []}
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(data, f)
            f.flush()
            with pytest.raises(GoldenDatasetValidationError) as exc_info:
                load_golden_dataset("synthetic", path=f.name)
            assert "validation failed" in str(exc_info.value).lower()


class TestLoadTestCases:
    """Tests for load_test_cases convenience function."""

    def test_load_test_cases_filters_by_category(self):
        """Test category filtering."""
        test_cases = load_test_cases("synthetic", category="basic")
        assert all(tc.category == "basic" for tc in test_cases)

    def test_load_test_cases_filters_by_difficulty(self):
        """Test difficulty filtering."""
        test_cases = load_test_cases("synthetic", difficulty="easy")
        assert all(tc.difficulty == "easy" for tc in test_cases)

    def test_load_test_cases_excludes_skipped_by_default(self):
        """Test that skipped cases are excluded by default."""
        all_cases = load_test_cases("synthetic", include_skipped=True)
        active_cases = load_test_cases("synthetic", include_skipped=False)
        # If there are skipped cases, active should be fewer
        # If none are skipped, counts should be equal
        assert len(active_cases) <= len(all_cases)
