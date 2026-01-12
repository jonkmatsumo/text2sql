"""Unit tests for pattern generator utilities."""

import pytest
from mcp_server.models import ColumnDef
from mcp_server.services.patterns.generator import (
    generate_column_patterns,
    generate_table_patterns,
    get_target_tables,
    normalize_name,
)


def test_normalize_name():
    """Test name normalization and variation generation."""
    # Basic snake_case
    assert "user account" in normalize_name("user_account")

    # Pluralization (simple heuristic)
    variations = normalize_name("user_account")
    assert "user accounts" in variations

    # ID handling
    id_vars = normalize_name("user_id")
    assert "user identifier" in id_vars
    assert "user id" in id_vars

    # Already spaced
    assert "first name" in normalize_name("first name")


@pytest.mark.asyncio
async def test_get_target_tables():
    """Test table filtering (denylist)."""

    class MockIntrospector:
        async def list_table_names(self, schema="public"):
            return [
                "users",
                "alembic_version",
                "flyway_schema_history",
                "spatial_ref_sys",
                "nlp_patterns",
                "orders",
            ]

    tables = await get_target_tables(MockIntrospector())
    assert "users" in tables
    assert "orders" in tables
    assert "alembic_version" not in tables
    assert "flyway_schema_history" not in tables
    assert "spatial_ref_sys" not in tables
    assert "nlp_patterns" not in tables


def test_generate_table_patterns():
    """Test generating patterns for a table."""
    patterns = generate_table_patterns("user_accounts")

    # Check structure
    assert all(p["label"] == "TABLE" for p in patterns)
    assert all(p["id"] == "user_accounts" for p in patterns)

    # Check content
    values = [p["pattern"] for p in patterns]
    assert "user accounts" in values
    assert "user_accounts" in values


def test_generate_column_patterns():
    """Test generating patterns for a column."""
    col = ColumnDef(name="first_name", data_type="text", is_nullable=True)
    patterns = generate_column_patterns("users", col)

    assert all(p["label"] == "COLUMN" for p in patterns)
    # ID should probably qualify the column? Or just be the column name?
    # Usually entity recognizers map to a canonical ID.
    # For columns, the canonical ID might be "table.column" or just "column" if unique.
    # Let's assume for now we want "table.column" or just "column".
    # Existing code used value as ID.
    # Plan didn't specify ID format rigidly, but "Determine canonical ID" is implied.
    # I'll use "table.column" as the ID for columns to be safe/unambiguous.
    assert all(p["id"] == "users.first_name" for p in patterns)

    values = [p["pattern"] for p in patterns]
    assert "first name" in values


def test_should_scan_column():
    """Test value scan heuristics."""
    from mcp_server.services.patterns.generator import should_scan_column

    # Enum-like types
    assert (
        should_scan_column(ColumnDef(name="some_enum", data_type="USER-DEFINED", is_nullable=True))
        is True
    )

    # Whitelisted names (text)
    assert should_scan_column(ColumnDef(name="status", data_type="text", is_nullable=True)) is True
    assert (
        should_scan_column(ColumnDef(name="order_status", data_type="varchar", is_nullable=True))
        is True
    )
    assert should_scan_column(ColumnDef(name="genre", data_type="text", is_nullable=True)) is True

    # Ignored names
    assert (
        should_scan_column(ColumnDef(name="description", data_type="text", is_nullable=True))
        is False
    )
    assert should_scan_column(ColumnDef(name="email", data_type="text", is_nullable=True)) is False

    # Ignored types (even if name matches)
    assert (
        should_scan_column(ColumnDef(name="status_id", data_type="integer", is_nullable=True))
        is False
    )


@pytest.mark.asyncio
async def test_fetch_distinct_values():
    """Test distinct value fetching."""
    from unittest.mock import AsyncMock, MagicMock

    from mcp_server.services.patterns.generator import fetch_distinct_values

    mock_conn = AsyncMock()
    # Mock row objects (subscriptable)
    mock_conn.fetch.return_value = [
        MagicMock(getitem=lambda self, x: "Active"),
        MagicMock(getitem=lambda self, x: "Inactive"),
    ]

    # Override getitem to behave like list/tuple/dict
    # Actually asyncpg Record behaves like a mapping + sequence
    # Let's just return tuples for simple mocking? asyncpg fetch returns Record objects.
    # The code does `row[0]`.
    mock_conn.fetch.return_value = [["Active"], ["Inactive"]]

    values = await fetch_distinct_values(mock_conn, "users", "status")

    assert "Active" in values
    assert "Inactive" in values
    assert len(values) == 2
