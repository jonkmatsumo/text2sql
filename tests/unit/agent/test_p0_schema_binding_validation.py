"""P0 schema binding validation tests."""

from unittest.mock import MagicMock, patch

import pytest
from langchain_core.messages import HumanMessage

from agent.nodes.validate import _build_schema_binding, _extract_identifiers, validate_sql_node


def test_build_schema_binding_map():
    """Build schema binding map from raw schema nodes."""
    raw_schema = [
        {"type": "Table", "name": "customers"},
        {"type": "Column", "name": "id", "table": "customers"},
        {"type": "Column", "name": "name", "table": "customers"},
        {"type": "Table", "name": "orders"},
        {"type": "Column", "name": "id", "table": "orders"},
    ]

    binding = _build_schema_binding(raw_schema)

    assert "customers" in binding
    assert "orders" in binding
    assert "id" in binding["customers"]
    assert "name" in binding["customers"]
    assert "id" in binding["orders"]


def test_extract_identifiers_simple():
    """Extract tables and qualified columns from SQL."""
    tables, columns = _extract_identifiers(
        "SELECT customers.id, customers.name FROM customers WHERE customers.id = 1"
    )

    assert "customers" in tables
    assert ("customers", "id") in columns
    assert ("customers", "name") in columns


@pytest.mark.asyncio
async def test_schema_binding_rejects_missing_column(monkeypatch):
    """Reject SQL when referenced columns are missing from bound schema."""
    monkeypatch.setenv("AGENT_SCHEMA_BINDING_VALIDATION", "true")
    raw_schema = [
        {"type": "Table", "name": "customers"},
        {"type": "Column", "name": "id", "table": "customers"},
    ]

    state = {
        "messages": [HumanMessage(content="Show customers")],
        "schema_context": "",
        "raw_schema_context": raw_schema,
        "current_sql": "SELECT customers.name FROM customers",
        "query_result": None,
        "error": None,
        "retry_count": 0,
    }

    with patch("agent.nodes.validate.telemetry.start_span") as mock_span:
        mock_span.return_value.__enter__ = MagicMock(return_value=MagicMock())
        mock_span.return_value.__exit__ = MagicMock(return_value=False)
        result = await validate_sql_node(state)

    assert result["error_category"] == "schema_binding"
    assert "missing columns" in result["error"]
