"""Tests for tables summary generation."""

from text2sql_synth.schema import GENERATION_ORDER
from text2sql_synth.tables_summary import generate_tables_summary


def test_generate_tables_summary_structure():
    """Test that generated summary has correct structure."""
    summaries = generate_tables_summary()
    assert isinstance(summaries, list)
    assert len(summaries) > 0

    for item in summaries:
        assert isinstance(item, dict)
        assert "table_name" in item
        assert "summary" in item
        assert isinstance(item["table_name"], str)
        assert isinstance(item["summary"], str)
        assert len(item["summary"]) > 10  # Reasonable description length


def test_generate_tables_summary_completeness():
    """Test that all tables in GENERATION_ORDER are present."""
    summaries = generate_tables_summary()
    generated_tables = {item["table_name"] for item in summaries}

    for table in GENERATION_ORDER:
        assert table in generated_tables, f"Table {table} missing from summaries"


def test_generate_tables_summary_content():
    """Test specific content for known tables."""
    summaries = generate_tables_summary()
    lookup = {item["table_name"]: item["summary"] for item in summaries}

    assert "dim_customer" in lookup
    assert "demographic" in lookup["dim_customer"].lower()

    assert "fact_transaction" in lookup
    assert "financial transactions" in lookup["fact_transaction"].lower()
