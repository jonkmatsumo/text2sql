"""Tests for cardinality detection and sampling in generator."""

from unittest.mock import AsyncMock, patch

import pytest
from mcp_server.services.patterns.generator import generate_entity_patterns

from schema import ColumnDef, TableDef


@pytest.mark.asyncio
async def test_cardinality_boundary():
    """Test threshold boundary (<= threshold included, > threshold excluded)."""
    # 1. Setup
    mock_conn = AsyncMock()
    mock_db_ctx = AsyncMock()
    mock_db_ctx.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_db_ctx.__aexit__ = AsyncMock(return_value=None)

    mock_introspector = AsyncMock()
    mock_introspector.list_table_names.return_value = ["boundary_test"]
    mock_introspector.get_table_def.return_value = TableDef(
        name="boundary_test",
        columns=[
            ColumnDef(name="col_equal", data_type="text", is_nullable=True),  # 10 values
            ColumnDef(name="col_over", data_type="text", is_nullable=True),  # 11 values
        ],
        foreign_keys=[],
    )

    # 2. Mock Data
    async def side_effect_fetch(query, *args):
        if '"col_equal"' in query:
            # Return 10 values
            return [[f"v{i}"] for i in range(10)]
        if '"col_over"' in query:
            # Return 11 values
            return [[f"v{i}"] for i in range(11)]
        return []

    mock_conn.fetch.side_effect = side_effect_fetch

    # 3. Execution (Threshold=10 default)
    with patch("dal.database.Database.get_connection", return_value=mock_db_ctx), patch(
        "dal.database.Database.get_schema_introspector",
        return_value=mock_introspector,
    ), patch("mcp_server.services.patterns.generator.get_openai_client", return_value=None):

        patterns = await generate_entity_patterns()

        # 4. Assertions
        equal_patterns = [p for p in patterns if p.get("label") == "COL_EQUAL"]
        # Should be included (10 values)
        assert len(equal_patterns) == 10

        over_patterns = [p for p in patterns if p.get("label") == "COL_OVER"]
        # Should be excluded (11 > 10)
        assert len(over_patterns) == 0


@pytest.mark.asyncio
async def test_timeout_handling():
    """Test that timeout (or query error) results in graceful skip."""
    mock_conn = AsyncMock()
    # Mock execute to maybe succeed for SET LOCAL, but fetch to fail
    mock_conn.execute.return_value = None
    mock_conn.fetch.side_effect = Exception("Query timed out")

    mock_db_ctx = AsyncMock()
    mock_db_ctx.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_db_ctx.__aexit__ = AsyncMock(return_value=None)

    mock_introspector = AsyncMock()
    mock_introspector.list_table_names.return_value = ["timeout_test"]
    mock_introspector.get_table_def.return_value = TableDef(
        name="timeout_test",
        columns=[ColumnDef(name="col1", data_type="text", is_nullable=True)],
        foreign_keys=[],
    )

    with patch("dal.database.Database.get_connection", return_value=mock_db_ctx), patch(
        "dal.database.Database.get_schema_introspector",
        return_value=mock_introspector,
    ), patch("mcp_server.services.patterns.generator.get_openai_client", return_value=None):

        patterns = await generate_entity_patterns()

        # Should not crash, just return empty for that column
        col_patterns = [p for p in patterns if p.get("label") == "COL1"]
        assert len(col_patterns) == 0


@pytest.mark.asyncio
async def test_sampling_timeout_setting():
    """Test that SET LOCAL statement_timeout is generated."""
    # We call sample_distinct_values directly to inspect execute calls
    from mcp_server.services.patterns.generator import sample_distinct_values

    mock_conn = AsyncMock()
    mock_conn.fetch.return_value = []  # Empty result

    # Call with timeout 5000ms
    await sample_distinct_values(
        mock_conn, "tbl", "col", threshold=10, sample_rows=100, timeout_ms=5000
    )

    # Validates that execute was called with SET LOCAL
    calls = mock_conn.execute.call_args_list
    assert any("SET LOCAL statement_timeout = '5000ms'" in str(c) for c in calls)
