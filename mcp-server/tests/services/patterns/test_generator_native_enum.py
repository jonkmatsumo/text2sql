"""Tests for native ENUM support in generator."""

from unittest.mock import AsyncMock, patch

import pytest
from mcp_server.services.patterns.generator import generate_entity_patterns

from schema import ColumnDef, TableDef


@pytest.mark.asyncio
async def test_native_enum_extraction():
    """Test that native ENUM columns use catalog lookup and bypass sampling."""
    # 1. Setup
    mock_conn = AsyncMock()
    mock_db_ctx = AsyncMock()
    mock_db_ctx.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_db_ctx.__aexit__ = AsyncMock(return_value=None)

    mock_introspector = AsyncMock()
    mock_introspector.list_table_names.return_value = ["enum_test"]
    mock_introspector.get_table_def.return_value = TableDef(
        name="enum_test",
        columns=[
            ColumnDef(name="status", data_type="USER-DEFINED", is_nullable=True),
        ],
        foreign_keys=[],
    )

    # 2. Mock Data
    async def side_effect_fetch(query, *args):
        # Native Catalog Query Check
        if "pg_enum" in query and "pg_type" in query:
            # Return native enum values
            # args should contain table_name, column_name if passed as params
            # In implementation: await conn.fetch(query, table_name, column_name)
            return [["native_active"], ["native_inactive"]]

        # Sampling Query Check
        if "SELECT DISTINCT" in query:
            # Should NOT be called for native enum if found
            return [["scanned_value"]]

        return []

    mock_conn.fetch.side_effect = side_effect_fetch

    # 3. Execution
    with patch("dal.database.Database.get_connection", return_value=mock_db_ctx), patch(
        "dal.database.Database.get_schema_introspector",
        return_value=mock_introspector,
    ), patch("mcp_server.services.patterns.generator.get_openai_client", return_value=None):

        patterns = await generate_entity_patterns()

        # 4. Assertions
        status_patterns = [p for p in patterns if p.get("label") == "STATUS"]
        assert len(status_patterns) == 2
        values = sorted([p["pattern"] for p in status_patterns])
        assert values == ["native_active", "native_inactive"]


@pytest.mark.asyncio
async def test_native_enum_fallback():
    """Test fallback to scanning if native enum lookup fails."""
    # If native scan returns empty, it should try scanning

    # 1. Setup
    mock_conn = AsyncMock()
    mock_db_ctx = AsyncMock()
    mock_db_ctx.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_db_ctx.__aexit__ = AsyncMock(return_value=None)

    mock_introspector = AsyncMock()
    mock_introspector.list_table_names.return_value = ["fallback_test"]
    mock_introspector.get_table_def.return_value = TableDef(
        name="fallback_test",
        columns=[
            ColumnDef(name="broken_enum", data_type="USER-DEFINED", is_nullable=True),
        ],
        foreign_keys=[],
    )

    async def side_effect_fetch(query, *args):
        if "pg_enum" in query:
            return []  # Simulate not found

        if "SELECT DISTINCT" in query:
            return [["scanned_A"], ["scanned_B"]]

        return []

    mock_conn.fetch.side_effect = side_effect_fetch

    with patch("dal.database.Database.get_connection", return_value=mock_db_ctx), patch(
        "dal.database.Database.get_schema_introspector",
        return_value=mock_introspector,
    ), patch("mcp_server.services.patterns.generator.get_openai_client", return_value=None):

        patterns = await generate_entity_patterns()

        col_patterns = [p for p in patterns if p.get("label") == "BROKEN_ENUM"]
        assert len(col_patterns) == 2
        assert "scanned_a" in [p["pattern"] for p in col_patterns]
