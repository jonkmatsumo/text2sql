"""Unit tests for seeding module."""

import json
from pathlib import Path

import pytest
from mcp_server.seeding.loader import (
    load_examples_for_vector_db,
    load_golden_dataset,
    load_queries_from_files,
)


class TestLoadQueriesFromFiles:
    """Tests for load_queries_from_files function."""

    def test_load_single_file_nested_format(self, tmp_path: Path):
        """Test loading from a single file with nested format."""
        data = {
            "queries": [
                {"question": "Q1", "query": "SELECT 1;"},
                {"question": "Q2", "query": "SELECT 2;"},
            ]
        }
        file_path = tmp_path / "queries.json"
        file_path.write_text(json.dumps(data))

        result = load_queries_from_files(["queries.json"], base_path=tmp_path)

        assert len(result) == 2
        assert result[0]["question"] == "Q1"
        assert result[1]["query"] == "SELECT 2;"

    def test_load_single_file_list_format(self, tmp_path: Path):
        """Test loading from a file with flat list format."""
        data = [
            {"question": "Q1", "query": "SELECT 1;"},
            {"question": "Q2", "query": "SELECT 2;"},
        ]
        file_path = tmp_path / "queries.json"
        file_path.write_text(json.dumps(data))

        result = load_queries_from_files(["queries.json"], base_path=tmp_path)

        assert len(result) == 2

    def test_load_multiple_files(self, tmp_path: Path):
        """Test loading from multiple files."""
        file1 = tmp_path / "file1.json"
        file1.write_text(json.dumps({"queries": [{"question": "Q1", "query": "S1"}]}))

        file2 = tmp_path / "file2.json"
        file2.write_text(json.dumps({"queries": [{"question": "Q2", "query": "S2"}]}))

        result = load_queries_from_files(["file1.json", "file2.json"], base_path=tmp_path)

        assert len(result) == 2
        questions = [q["question"] for q in result]
        assert "Q1" in questions
        assert "Q2" in questions

    def test_load_glob_pattern(self, tmp_path: Path):
        """Test loading with glob pattern."""
        subdir = tmp_path / "queries"
        subdir.mkdir()

        (subdir / "a.json").write_text(json.dumps({"queries": [{"question": "QA", "query": "SA"}]}))
        (subdir / "b.json").write_text(json.dumps({"queries": [{"question": "QB", "query": "SB"}]}))

        result = load_queries_from_files(["queries/*.json"], base_path=tmp_path)

        assert len(result) == 2

    def test_file_not_found(self, tmp_path: Path):
        """Test error when no files match pattern."""
        with pytest.raises(FileNotFoundError, match="No files found"):
            load_queries_from_files(["nonexistent.json"], base_path=tmp_path)

    def test_invalid_json(self, tmp_path: Path):
        """Test error on invalid JSON."""
        file_path = tmp_path / "bad.json"
        file_path.write_text("not valid json")

        with pytest.raises(json.JSONDecodeError):
            load_queries_from_files(["bad.json"], base_path=tmp_path)

    def test_invalid_format(self, tmp_path: Path):
        """Test error on invalid data format."""
        file_path = tmp_path / "bad.json"
        file_path.write_text(json.dumps({"invalid": "format"}))

        with pytest.raises(ValueError, match="Invalid format"):
            load_queries_from_files(["bad.json"], base_path=tmp_path)

    def test_required_fields_validation(self, tmp_path: Path):
        """Test validation of required fields."""
        data = {"queries": [{"question": "Q1"}]}  # Missing 'query'
        file_path = tmp_path / "queries.json"
        file_path.write_text(json.dumps(data))

        with pytest.raises(ValueError, match="missing required fields"):
            load_queries_from_files(
                ["queries.json"],
                base_path=tmp_path,
                required_fields=["question", "query"],
            )

    def test_absolute_path(self, tmp_path: Path):
        """Test loading with absolute file path."""
        data = {"queries": [{"question": "Q1", "query": "S1"}]}
        file_path = tmp_path / "queries.json"
        file_path.write_text(json.dumps(data))

        result = load_queries_from_files([str(file_path)])

        assert len(result) == 1


class TestLoadExamplesForVectorDb:
    """Tests for load_examples_for_vector_db function."""

    def test_extracts_question_and_query_only(self, tmp_path: Path):
        """Test that only question and query are extracted."""
        data = {
            "queries": [
                {
                    "question": "Q1",
                    "query": "SELECT 1;",
                    "expected_result": [{"col": 1}],
                    "difficulty": "easy",
                    "category": "aggregation",
                }
            ]
        }
        file_path = tmp_path / "queries.json"
        file_path.write_text(json.dumps(data))

        result = load_examples_for_vector_db(["queries.json"], base_path=tmp_path)

        assert len(result) == 1
        assert set(result[0].keys()) == {"question", "query"}
        assert result[0]["question"] == "Q1"
        assert result[0]["query"] == "SELECT 1;"


class TestLoadGoldenDataset:
    """Tests for load_golden_dataset function."""

    def test_includes_all_metadata(self, tmp_path: Path):
        """Test that all metadata fields are included."""
        data = {
            "queries": [
                {
                    "question": "Q1",
                    "query": "SELECT 1;",
                    "expected_result": [{"col": 1}],
                    "expected_row_count": 1,
                    "difficulty": "hard",
                    "category": "aggregation",
                }
            ]
        }
        file_path = tmp_path / "queries.json"
        file_path.write_text(json.dumps(data))

        result = load_golden_dataset(["queries.json"], base_path=tmp_path, tenant_id=42)

        assert len(result) == 1
        tc = result[0]
        assert tc["question"] == "Q1"
        assert tc["ground_truth_sql"] == "SELECT 1;"
        assert tc["expected_result"] == [{"col": 1}]
        assert tc["expected_row_count"] == 1
        assert tc["difficulty"] == "hard"
        assert tc["category"] == "aggregation"
        assert tc["tenant_id"] == 42

    def test_default_values(self, tmp_path: Path):
        """Test default values for missing optional fields."""
        data = {"queries": [{"question": "Q1", "query": "SELECT 1;"}]}
        file_path = tmp_path / "queries.json"
        file_path.write_text(json.dumps(data))

        result = load_golden_dataset(["queries.json"], base_path=tmp_path)

        tc = result[0]
        assert tc["difficulty"] == "medium"
        assert tc["category"] == "general"
        assert tc["tenant_id"] == 1
        assert tc["expected_result"] is None
        assert tc["expected_row_count"] is None

    def test_query_level_tenant_id_override(self, tmp_path: Path):
        """Test that query-level tenant_id overrides default."""
        data = {"queries": [{"question": "Q1", "query": "SELECT 1;", "tenant_id": 99}]}
        file_path = tmp_path / "queries.json"
        file_path.write_text(json.dumps(data))

        result = load_golden_dataset(["queries.json"], base_path=tmp_path, tenant_id=1)

        assert result[0]["tenant_id"] == 99
