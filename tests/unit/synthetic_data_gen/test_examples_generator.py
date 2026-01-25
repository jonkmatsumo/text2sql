"""Tests for examples generator."""

import json
from unittest.mock import MagicMock

from synthetic_data_gen.cli import cmd_export_examples
from synthetic_data_gen.examples_generator import generate_examples


def test_generate_examples_structure():
    """Test examples structure."""
    examples = generate_examples()
    assert isinstance(examples, dict)
    assert len(examples) > 0

    for filename, content in examples.items():
        assert filename.endswith(".json")
        assert isinstance(content, dict)
        assert "question" in content
        assert "query" in content
        assert "category" in content
        assert "tenant_id" in content


def test_examples_sql_validity_mock():
    """Simple check that SQL looks like SQL (starts with SELECT)."""
    examples = generate_examples()
    for _, content in examples.items():
        sql = content["query"].upper()
        assert sql.startswith("SELECT") or sql.startswith("WITH")
        assert ";" in sql


def test_cmd_export_examples(tmp_path):
    """Test CLI export command."""
    args = MagicMock()
    args.out = str(tmp_path)

    assert cmd_export_examples(args) == 0

    # Check files exist
    files = list(tmp_path.glob("*.json"))
    assert len(files) > 0

    # Check content
    with open(files[0]) as f:
        content = json.load(f)
        assert "question" in content
        assert "query" in content
