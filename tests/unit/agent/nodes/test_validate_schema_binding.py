"""Tests for schema binding validation telemetry."""

from unittest.mock import MagicMock, patch

import pytest

from agent.nodes.validate import validate_sql_node
from agent.state import AgentState


@pytest.fixture
def mock_telemetry(mocker):
    """Mock telemetry."""
    return mocker.patch("agent.nodes.validate.telemetry")


@pytest.fixture
def mock_validate_sql(mocker):
    """Mock validate_sql to return success."""
    # Mock successful AST validation by default
    mock_res = MagicMock()
    mock_res.is_valid = True
    mock_res.metadata = MagicMock()
    mock_res.metadata.table_lineage = []
    mock_res.metadata.column_usage = []
    mock_res.to_dict.return_value = {"is_valid": True}
    return mocker.patch("agent.nodes.validate.validate_sql", return_value=mock_res)


@pytest.mark.asyncio
async def test_validate_schema_binding_success(mock_telemetry, mock_validate_sql):
    """Test schema binding enabled and passes with valid schema."""
    state = AgentState(
        messages=[],
        current_sql="SELECT name FROM users",
        # Mock schema context: table 'users' with col 'name'
        raw_schema_context=[
            {"type": "Table", "name": "users"},
            {"type": "Column", "table": "users", "name": "name"},
        ],
    )

    with patch(
        "agent.nodes.validate.get_env_bool",
        side_effect=lambda k, d: True if k == "AGENT_SCHEMA_BINDING_VALIDATION" else d,
    ):
        result = await validate_sql_node(state)

    assert result.get("error") is None

    # Verify span attributes
    span = mock_telemetry.start_span.return_value.__enter__.return_value
    # Should resolve binding
    span.set_attribute.assert_any_call("validation.schema_bound", True)


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

    with patch(
        "agent.nodes.validate.get_env_bool",
        side_effect=lambda k, d: True if k == "AGENT_SCHEMA_BINDING_VALIDATION" else d,
    ):
        result = await validate_sql_node(state)

    assert "missing tables: unknown_table" in result["error"]
    assert result["error_category"] == "schema_binding"

    span = mock_telemetry.start_span.return_value.__enter__.return_value
    span.set_attribute.assert_any_call("validation.missing_tables", "unknown_table")


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

    with patch(
        "agent.nodes.validate.get_env_bool",
        side_effect=lambda k, d: True if k == "AGENT_SCHEMA_BINDING_VALIDATION" else d,
    ):
        result = await validate_sql_node(state)

    assert "missing columns: users.unknown_col" in result["error"]

    span = mock_telemetry.start_span.return_value.__enter__.return_value
    span.set_attribute.assert_any_call("validation.missing_columns", "users.unknown_col")
