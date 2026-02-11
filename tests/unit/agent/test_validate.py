"""Unit tests for SQL validation node."""

from unittest.mock import patch

import pytest
from langchain_core.messages import HumanMessage

from agent.nodes.validate import validate_sql_node


@pytest.fixture
def base_state():
    """Create a base agent state for testing."""
    return {
        "messages": [HumanMessage(content="Show me all customers")],
        "schema_context": "Table: customers (id, name, email)",
        "table_names": ["customers", "orders"],
        "current_sql": None,
        "query_result": None,
        "error": None,
        "retry_count": 0,
        "tenant_id": 1,
    }


class TestValidateSqlNode:
    """Tests for validate_sql_node function."""

    @pytest.mark.asyncio
    async def test_valid_query_passes(self, base_state):
        """Test that valid SQL passes validation."""
        base_state["current_sql"] = "SELECT * FROM customers"

        with patch("agent.nodes.validate.telemetry.start_span") as mock_span:
            mock_span.return_value.__enter__ = lambda s: type(
                "Span",
                (),
                {
                    "set_inputs": lambda *a, **k: None,
                    "set_outputs": lambda *a, **k: None,
                    "set_attribute": lambda *a, **k: None,
                },
            )()
            mock_span.return_value.__exit__ = lambda *a, **k: None

            result = await validate_sql_node(base_state)

        assert result.get("error") is None
        assert result.get("ast_validation_result") is not None
        assert result["ast_validation_result"]["is_valid"] is True

    @pytest.mark.asyncio
    async def test_restricted_table_fails(self, base_state):
        """Test that query with restricted table fails validation."""
        base_state["current_sql"] = "SELECT * FROM payroll"

        with patch("agent.nodes.validate.telemetry.start_span") as mock_span:
            mock_span.return_value.__enter__ = lambda s: type(
                "Span",
                (),
                {
                    "set_inputs": lambda *a, **k: None,
                    "set_outputs": lambda *a, **k: None,
                    "set_attribute": lambda *a, **k: None,
                },
            )()
            mock_span.return_value.__exit__ = lambda *a, **k: None

            result = await validate_sql_node(base_state)

        assert result.get("error") is not None
        assert "restricted" in result["error"].lower() or "security" in result["error"].lower()
        assert result["ast_validation_result"]["is_valid"] is False

    @pytest.mark.asyncio
    async def test_forbidden_command_fails(self, base_state):
        """Test that DROP command fails validation."""
        base_state["current_sql"] = "DROP TABLE customers"

        with patch("agent.nodes.validate.telemetry.start_span") as mock_span:
            mock_span.return_value.__enter__ = lambda s: type(
                "Span",
                (),
                {
                    "set_inputs": lambda *a, **k: None,
                    "set_outputs": lambda *a, **k: None,
                    "set_attribute": lambda *a, **k: None,
                },
            )()
            mock_span.return_value.__exit__ = lambda *a, **k: None

            result = await validate_sql_node(base_state)

        assert result.get("error") is not None
        assert "forbidden" in result["error"].lower()

    @pytest.mark.asyncio
    async def test_non_allowlisted_table_fails(self, base_state):
        """Test that table not in schema-derived allowlist is rejected."""
        base_state["current_sql"] = "SELECT * FROM products"

        with patch("agent.nodes.validate.telemetry.start_span") as mock_span:
            mock_span.return_value.__enter__ = lambda s: type(
                "Span",
                (),
                {
                    "set_inputs": lambda *a, **k: None,
                    "set_outputs": lambda *a, **k: None,
                    "set_attribute": lambda *a, **k: None,
                },
            )()
            mock_span.return_value.__exit__ = lambda *a, **k: None

            result = await validate_sql_node(base_state)

        assert result.get("error") is not None
        assert "allowlist" in result["error"].lower()

    @pytest.mark.asyncio
    async def test_no_sql_returns_error(self, base_state):
        """Test that missing SQL returns error."""
        base_state["current_sql"] = None

        with patch("agent.nodes.validate.telemetry.start_span") as mock_span:
            mock_span.return_value.__enter__ = lambda s: type(
                "Span",
                (),
                {
                    "set_inputs": lambda *a, **k: None,
                    "set_outputs": lambda *a, **k: None,
                    "set_attribute": lambda *a, **k: None,
                },
            )()
            mock_span.return_value.__exit__ = lambda *a, **k: None

            result = await validate_sql_node(base_state)

        assert result.get("error") is not None
        assert "no sql" in result["error"].lower()

    @pytest.mark.asyncio
    async def test_metadata_extraction(self, base_state):
        """Test that metadata is extracted from valid query."""
        base_state[
            "current_sql"
        ] = """
            SELECT c.name, o.amount
            FROM customers c
            JOIN orders o ON c.id = o.customer_id
        """

        with patch("agent.nodes.validate.telemetry.start_span") as mock_span:
            mock_span.return_value.__enter__ = lambda s: type(
                "Span",
                (),
                {
                    "set_inputs": lambda *a, **k: None,
                    "set_outputs": lambda *a, **k: None,
                    "set_attribute": lambda *a, **k: None,
                },
            )()
            mock_span.return_value.__exit__ = lambda *a, **k: None

            result = await validate_sql_node(base_state)

        assert result.get("table_lineage") is not None
        assert len(result["table_lineage"]) >= 2
        assert result.get("join_complexity") == 1
        assert result.get("query_join_count") == 1
        assert result.get("query_estimated_table_count") >= 2
        assert result.get("query_estimated_scan_columns") >= 2
        assert result.get("query_complexity_score") >= 7

    @pytest.mark.asyncio
    async def test_metadata_preserved_on_failure(self, base_state):
        """Test that metadata is preserved even when validation fails."""
        base_state["current_sql"] = "SELECT * FROM payroll"

        with patch("agent.nodes.validate.telemetry.start_span") as mock_span:
            mock_span.return_value.__enter__ = lambda s: type(
                "Span",
                (),
                {
                    "set_inputs": lambda *a, **k: None,
                    "set_outputs": lambda *a, **k: None,
                    "set_attribute": lambda *a, **k: None,
                },
            )()
            mock_span.return_value.__exit__ = lambda *a, **k: None

            result = await validate_sql_node(base_state)

        # Metadata should still be extracted for audit purposes
        assert result.get("table_lineage") is not None
        assert "payroll" in result["table_lineage"]

    @pytest.mark.asyncio
    async def test_complex_query_validation(self, base_state):
        """Test validation of complex query with CTE and window functions."""
        base_state[
            "current_sql"
        ] = """
            WITH ranked_customers AS (
                SELECT
                    customer_id,
                    SUM(amount) as total,
                    ROW_NUMBER() OVER (ORDER BY SUM(amount) DESC) as rank
                FROM orders
                GROUP BY customer_id
            )
            SELECT * FROM ranked_customers WHERE rank <= 10
        """

        with patch("agent.nodes.validate.telemetry.start_span") as mock_span:
            mock_span.return_value.__enter__ = lambda s: type(
                "Span",
                (),
                {
                    "set_inputs": lambda *a, **k: None,
                    "set_outputs": lambda *a, **k: None,
                    "set_attribute": lambda *a, **k: None,
                },
            )()
            mock_span.return_value.__exit__ = lambda *a, **k: None

            result = await validate_sql_node(base_state)

        assert result.get("error") is None
        assert result["ast_validation_result"]["is_valid"] is True

    @pytest.mark.asyncio
    async def test_column_allowlist_from_schema_context_blocks_projection(
        self, base_state, monkeypatch
    ):
        """Block mode should reject projected columns not present in schema context allowlist."""
        monkeypatch.setenv("AGENT_COLUMN_ALLOWLIST_MODE", "block")
        monkeypatch.setenv("AGENT_COLUMN_ALLOWLIST_FROM_SCHEMA_CONTEXT", "true")
        monkeypatch.setenv("AGENT_SCHEMA_BINDING_VALIDATION", "false")
        base_state["raw_schema_context"] = [
            {"type": "Table", "name": "customers"},
            {"type": "Column", "table": "customers", "name": "id"},
            {"type": "Column", "table": "customers", "name": "name"},
        ]
        base_state["current_sql"] = "SELECT customers.email FROM customers"

        with patch("agent.nodes.validate.telemetry.start_span") as mock_span:
            mock_span.return_value.__enter__ = lambda s: type(
                "Span",
                (),
                {
                    "set_inputs": lambda *a, **k: None,
                    "set_outputs": lambda *a, **k: None,
                    "set_attribute": lambda *a, **k: None,
                },
            )()
            mock_span.return_value.__exit__ = lambda *a, **k: None

            result = await validate_sql_node(base_state)

        assert result.get("error") is not None
        assert "column allowlist" in result["error"].lower()
        assert result["ast_validation_result"]["is_valid"] is False

    @pytest.mark.asyncio
    async def test_column_allowlist_warn_mode_allows_execution(self, base_state, monkeypatch):
        """Warn mode should not invalidate SQL even if a projected column is not allowlisted."""
        monkeypatch.setenv("AGENT_COLUMN_ALLOWLIST_MODE", "warn")
        monkeypatch.setenv("AGENT_COLUMN_ALLOWLIST_FROM_SCHEMA_CONTEXT", "true")
        monkeypatch.setenv("AGENT_SCHEMA_BINDING_VALIDATION", "false")
        base_state["raw_schema_context"] = [
            {"type": "Table", "name": "customers"},
            {"type": "Column", "table": "customers", "name": "id"},
        ]
        base_state["current_sql"] = "SELECT customers.email FROM customers"

        with patch("agent.nodes.validate.telemetry.start_span") as mock_span:
            mock_span.return_value.__enter__ = lambda s: type(
                "Span",
                (),
                {
                    "set_inputs": lambda *a, **k: None,
                    "set_outputs": lambda *a, **k: None,
                    "set_attribute": lambda *a, **k: None,
                },
            )()
            mock_span.return_value.__exit__ = lambda *a, **k: None

            result = await validate_sql_node(base_state)

        assert result.get("error") is None
        assert result["ast_validation_result"]["is_valid"] is True
        assert result["ast_validation_result"]["warnings"]

    @pytest.mark.asyncio
    async def test_cartesian_join_block_mode_rejects_query(self, base_state, monkeypatch):
        """Block mode should reject likely Cartesian joins."""
        monkeypatch.setenv("AGENT_SCHEMA_BINDING_VALIDATION", "false")
        monkeypatch.setenv("AGENT_CARTESIAN_JOIN_MODE", "block")
        base_state["current_sql"] = "SELECT * FROM customers CROSS JOIN orders"

        with patch("agent.nodes.validate.telemetry.start_span") as mock_span:
            mock_span.return_value.__enter__ = lambda s: type(
                "Span",
                (),
                {
                    "set_inputs": lambda *a, **k: None,
                    "set_outputs": lambda *a, **k: None,
                    "set_attribute": lambda *a, **k: None,
                },
            )()
            mock_span.return_value.__exit__ = lambda *a, **k: None

            result = await validate_sql_node(base_state)

        assert result.get("error") is not None
        assert "cartesian join" in result["error"].lower()
        assert result["ast_validation_result"]["is_valid"] is False
