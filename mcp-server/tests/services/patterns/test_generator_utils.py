"""Unit tests for pattern generator utilities."""

import pytest
from mcp_server.services.patterns.generator import (
    generate_column_patterns,
    generate_table_patterns,
    get_target_tables,
    normalize_name,
)

from schema import ColumnDef


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


@pytest.mark.asyncio
async def test_fetch_distinct_values_replacement():
    """Test sample_distinct_values fetching."""
    from unittest.mock import AsyncMock

    from mcp_server.services.patterns.generator import sample_distinct_values

    mock_conn = AsyncMock()
    mock_conn.fetch.return_value = [["Active"], ["Inactive"]]

    values = await sample_distinct_values(mock_conn, "users", "status", threshold=10)

    assert "Inactive" in values
    assert len(values) == 2


@pytest.mark.asyncio
async def test_enrich_with_retry():
    """Test enrichment retry logic."""
    from unittest.mock import AsyncMock, MagicMock, patch

    from mcp_server.services.patterns.generator import enrich_values_with_llm

    client = MagicMock()
    client.chat.completions.create = AsyncMock()

    # Fail twice, succeed third time
    # Note: Structure of response mock needs to match generator expectations
    choice_mock = MagicMock()
    choice_mock.message.content = '{"items": [{"pattern": "p", "id": "v"}]}'
    success_response = MagicMock()
    success_response.choices = [choice_mock]

    client.chat.completions.create.side_effect = [
        Exception("Fail 1"),
        Exception("Fail 2"),
        success_response,
    ]

    # We patch sleep to speed up test
    with patch("asyncio.sleep", new_callable=AsyncMock):
        patterns = await enrich_values_with_llm(client, "L", ["v"], run_id="test-run")

    assert len(patterns) == 1
    assert client.chat.completions.create.call_count == 3
