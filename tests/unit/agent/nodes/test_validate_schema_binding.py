"""Tests for schema binding validation telemetry."""

from unittest.mock import MagicMock, patch

import pytest

from agent.nodes.validate import validate_sql_node
from agent.state import AgentState


@pytest.fixture
def mock_telemetry():
    """Mock telemetry."""
    with patch("agent.nodes.validate.telemetry") as mock:
        yield mock


@pytest.fixture
def mock_validate_sql():
    """Mock validate_sql to return success."""
    mock_res = MagicMock()
    mock_res.is_valid = True
    mock_res.metadata = MagicMock()
    mock_res.metadata.table_lineage = []
    mock_res.metadata.column_usage = []
    mock_res.metadata.join_complexity = 0
    mock_res.parsed_sql = None
    mock_res.to_dict.return_value = {"is_valid": True}
    mock_res.violations = []
    with patch("agent.nodes.validate.validate_sql", return_value=mock_res):
        yield mock_res


@pytest.mark.asyncio
async def test_validate_schema_binding_success(mock_telemetry, mock_validate_sql):
    """Test schema binding enabled and passes with valid schema."""
    state = AgentState(
        messages=[],
        current_sql="SELECT name FROM users",
        raw_schema_context=[
            {"type": "Table", "name": "users"},
            {"type": "Column", "table": "users", "name": "name"},
        ],
    )

    # Default is now True, so no patch needed
    result = await validate_sql_node(state)

    assert result.get("error") is None

    span = mock_telemetry.start_span.return_value.__enter__.return_value
    span.set_attribute.assert_any_call("validation.schema_bound", True)
    span.set_attribute.assert_any_call("validation.schema_bound_enabled", True)


@pytest.mark.asyncio
async def test_validate_schema_binding_missing_table(mock_telemetry, mock_validate_sql):
    """Test schema binding fails for missing table."""
    state = AgentState(
        messages=[],
        current_sql="SELECT * FROM unknown_table",
        raw_schema_context=[
            {"type": "Table", "name": "users"},
        ],
    )

    # Default is True (hard mode)
    result = await validate_sql_node(state)

    assert "missing tables: unknown_table" in result["error"]
    assert result["error_category"] == "schema_binding"

    span = mock_telemetry.start_span.return_value.__enter__.return_value
    span.set_attribute.assert_any_call("validation.missing_tables", "unknown_table")
    span.set_attribute.assert_any_call("validation.schema_bound_blocked", True)


@pytest.mark.asyncio
async def test_validate_schema_binding_missing_column(mock_telemetry, mock_validate_sql):
    """Test schema binding fails for missing column."""
    state = AgentState(
        messages=[],
        current_sql="SELECT users.unknown_col FROM users",
        raw_schema_context=[
            {"type": "Table", "name": "users"},
            {"type": "Column", "table": "users", "name": "known_col"},
        ],
    )

    result = await validate_sql_node(state)

    assert "missing columns: users.unknown_col" in result["error"]

    span = mock_telemetry.start_span.return_value.__enter__.return_value
    span.set_attribute.assert_any_call("validation.missing_columns", "users.unknown_col")


@pytest.mark.asyncio
async def test_validate_schema_binding_soft_mode(mock_telemetry, mock_validate_sql):
    """Test schema binding in soft mode warns but doesn't block."""
    state = AgentState(
        messages=[],
        current_sql="SELECT * FROM unknown_table",
        raw_schema_context=[
            {"type": "Table", "name": "users"},
        ],
    )

    # Enable soft mode
    with patch(
        "agent.nodes.validate.get_env_bool",
        side_effect=lambda k, d: (
            True
            if k == "AGENT_SCHEMA_BINDING_VALIDATION"
            else True if k == "AGENT_SCHEMA_BINDING_SOFT_MODE" else d
        ),
    ):
        result = await validate_sql_node(state)

    # Should NOT block - error should be None (continues to AST validation)
    assert result.get("error_category") != "schema_binding"

    span = mock_telemetry.start_span.return_value.__enter__.return_value
    span.set_attribute.assert_any_call("validation.schema_bound_blocked", False)
    span.add_event.assert_called()
