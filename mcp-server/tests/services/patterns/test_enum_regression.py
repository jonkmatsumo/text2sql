"""Regression tests for enum detection / high-cardinality exclusion."""

from unittest.mock import AsyncMock, patch

import pytest
from mcp_server.services.patterns.generator import generate_entity_patterns


@pytest.mark.asyncio
async def test_high_cardinality_exclusion_regression():
    """Test that high-cardinality columns are EXCLUDED from value generation.

    This checks the requirement:
    "String columns are treated as enum-like only if COUNT(DISTINCT) < N".
    """
    from schema import ColumnDef, TableDef

    # Setup Mocks
    mock_conn = AsyncMock()
    mock_db_ctx = AsyncMock()
    mock_db_ctx.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_db_ctx.__aexit__ = AsyncMock(return_value=None)

    mock_introspector = AsyncMock()
    mock_introspector.list_table_names.return_value = ["users"]

    # Define table with:
    # 1. 'status' -> Low cardinality (3 values) -> Should be INCLUDED
    # 2. 'user_type' -> High card (15 values) -> Should be EXCLUDED (assuming threshold 10)
    mock_introspector.get_table_def.return_value = TableDef(
        name="users",
        columns=[
            ColumnDef(name="id", data_type="integer", is_nullable=False),
            ColumnDef(name="status", data_type="text", is_nullable=True),
            ColumnDef(name="user_type", data_type="text", is_nullable=True),
        ],
        foreign_keys=[],
        description="User table",
    )

    # Mock fetch distinct values
    async def side_effect_fetch(query, limit=None):
        if '"status"' in query:
            return [["active"], ["inactive"], ["pending"]]
        if '"user_type"' in query:
            return [[f"type_{i}"] for i in range(15)]
        return []

    mock_conn.fetch.side_effect = side_effect_fetch

    with patch(
        "mcp_server.config.database.Database.get_connection", return_value=mock_db_ctx
    ), patch(
        "mcp_server.config.database.Database.get_schema_introspector",
        return_value=mock_introspector,
    ), patch(
        "mcp_server.services.patterns.generator.get_openai_client", return_value=None
    ):
        patterns = await generate_entity_patterns()

        # Check STATUS patterns (should exist)
        status_patterns = [p for p in patterns if p.get("label") == "STATUS"]
        assert len(status_patterns) == 3, "Expected 3 patterns for 'status' column"

        # Check TABLE patterns (should exist - regression check)
        table_patterns = [p for p in patterns if p.get("label") == "TABLE"]
        assert len(table_patterns) > 0
        assert any(p["pattern"] == "users" for p in table_patterns)

        # Check USER_TYPE patterns (should NOT exist because 15 > threshold)
        user_type_patterns = [p for p in patterns if p.get("label") == "USER_TYPE"]
        assert (
            len(user_type_patterns) == 0
        ), f"Expected 0 patterns for 'user_type', got {len(user_type_patterns)}"
