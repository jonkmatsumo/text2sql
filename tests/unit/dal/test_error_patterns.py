"""Tests for provider-specific schema drift error patterns."""

import pytest

from dal.error_patterns import extract_missing_identifiers


@pytest.mark.parametrize(
    ("provider", "message", "expected"),
    [
        ("postgres", 'relation "public.users" does not exist', ["public.users"]),
        ("postgres", 'column "email" does not exist', ["email"]),
        ("redshift", 'table "sales" does not exist', ["sales"]),
        ("sqlite", "no such table: customers", ["customers"]),
        ("sqlite", "no such column: total", ["total"]),
        ("bigquery", "Not found: Table proj.dataset.table", ["proj.dataset.table"]),
        ("bigquery", "Unrecognized name: order_id", ["order_id"]),
        (
            "snowflake",
            "SQL compilation error: Object 'DB.SCHEMA.TBL' does not exist",
            ["DB.SCHEMA.TBL"],
        ),
        (
            "databricks",
            "Table or view not found: analytics.events",
            ["analytics.events"],
        ),
        (
            "databricks",
            "[TABLE_OR_VIEW_NOT_FOUND] The table or view `main.default.missing` cannot be found",
            ["main.default.missing"],
        ),
        ("athena", "Column 'user_id' cannot be resolved", ["user_id"]),
        ("clickhouse", "DB::Exception: Table db.table doesn't exist", ["db.table"]),
        ("mysql", "Table 'my_db.missing_table' doesn't exist", ["my_db.missing_table"]),
        ("mysql", "Unknown column 'secret_col' in 'field list'", ["secret_col"]),
        ("duckdb", "Table with name missing_table does not exist", ["missing_table"]),
        ("duckdb", 'Referenced column "missing_col" not found', ["missing_col"]),
    ],
)
def test_extract_missing_identifiers_positive(provider, message, expected):
    """Identifiers should be extracted for missing table/column errors."""
    assert extract_missing_identifiers(provider, message) == expected


@pytest.mark.parametrize(
    ("provider", "message"),
    [
        ("postgres", "permission denied for relation users"),
        ("redshift", "not authorized to access relation"),
        ("bigquery", "Quota exceeded: Too many requests"),
        ("snowflake", "Object 'DB.SCHEMA.TBL' does not exist or not authorized"),
        ("databricks", "PERMISSION_DENIED: User does not have access"),
        ("athena", "Query exhausted resources at this scale factor"),
        ("clickhouse", "Code: 60, e.displayText() = DB::Exception: Not enough privileges"),
    ],
)
def test_extract_missing_identifiers_negative(provider, message):
    """Non-schema errors should not be classified as drift."""
    assert extract_missing_identifiers(provider, message) == []
