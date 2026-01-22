"""Tests for SQL export functionality."""

import pandas as pd

from text2sql_synth.sql_export import (
    _escape_sql_value,
    _infer_pg_type,
    generate_data_sql,
    generate_schema_sql,
)


class TestEscapeSqlValue:
    """Tests for SQL value escaping."""

    def test_escape_null(self):
        """Test NULL handling."""
        assert _escape_sql_value(None) == "NULL"
        assert _escape_sql_value(pd.NA) == "NULL"

    def test_escape_boolean(self):
        """Test boolean escaping."""
        assert _escape_sql_value(True) == "TRUE"
        assert _escape_sql_value(False) == "FALSE"

    def test_escape_numbers(self):
        """Test numeric escaping."""
        assert _escape_sql_value(42) == "42"
        assert _escape_sql_value(3.14) == "3.14"

    def test_escape_string(self):
        """Test string escaping."""
        assert _escape_sql_value("hello") == "'hello'"
        assert _escape_sql_value("it's") == "'it''s'"

    def test_escape_json(self):
        """Test JSON escaping."""
        result = _escape_sql_value({"key": "value"})
        assert result.startswith("'{")
        assert "key" in result


class TestInferPgType:
    """Tests for PostgreSQL type inference."""

    def test_id_column(self):
        """Test ID column inference."""
        series = pd.Series([1, 2, 3])
        assert _infer_pg_type("customer_id", "int64", series) == "BIGINT"
        assert _infer_pg_type("uuid_id", "object", series) == "TEXT"

    def test_timestamp_column(self):
        """Test timestamp column inference."""
        series = pd.Series(["2026-01-01"])
        assert _infer_pg_type("created_ts", "object", series) == "TIMESTAMP"
        assert _infer_pg_type("created_date", "object", series) == "TIMESTAMP"

    def test_amount_column(self):
        """Test amount column inference."""
        series = pd.Series([100.50])
        assert _infer_pg_type("gross_amount", "float64", series) == "DECIMAL(15, 2)"
        assert _infer_pg_type("processing_fee", "float64", series) == "DECIMAL(15, 2)"

    def test_boolean_column(self):
        """Test boolean column inference."""
        series = pd.Series([True, False])
        assert _infer_pg_type("is_active", "bool", series) == "BOOLEAN"


class MockGenerationContext:
    """Mock context for testing."""

    def __init__(self, tables):
        """Initialize with mock tables."""
        self.tables = tables


class TestGenerateSchemaSql:
    """Tests for schema SQL generation."""

    def test_schema_sql_structure(self):
        """Test basic schema SQL structure."""
        ctx = MockGenerationContext(
            {
                "dim_time": pd.DataFrame(
                    {"date_key": [1, 2], "full_date": ["2026-01-01", "2026-01-02"]}
                )
            }
        )
        sql = generate_schema_sql(ctx)

        assert "CREATE TABLE IF NOT EXISTS dim_time" in sql
        assert "date_key" in sql
        assert "full_date" in sql

    def test_schema_sql_order(self):
        """Test that tables are created in generation order."""
        ctx = MockGenerationContext(
            {
                "dim_customer": pd.DataFrame({"customer_id": [1]}),
                "dim_time": pd.DataFrame({"date_key": [1]}),
            }
        )
        sql = generate_schema_sql(ctx)

        # dim_time should come before dim_customer
        time_pos = sql.find("dim_time")
        customer_pos = sql.find("dim_customer")
        assert time_pos < customer_pos


class TestGenerateDataSql:
    """Tests for data SQL generation."""

    def test_data_sql_structure(self):
        """Test basic data SQL structure."""
        ctx = MockGenerationContext({"dim_time": pd.DataFrame({"date_key": [1], "year": [2026]})})
        sql = generate_data_sql(ctx)

        assert "INSERT INTO dim_time" in sql
        assert "date_key, year" in sql
        assert "1" in sql
        assert "2026" in sql

    def test_data_sql_escaping(self):
        """Test that values are properly escaped."""
        ctx = MockGenerationContext(
            {"dim_time": pd.DataFrame({"date_key": [1], "name": ["O'Brien"]})}
        )
        sql = generate_data_sql(ctx)

        # Single quotes should be escaped
        assert "O''Brien" in sql
