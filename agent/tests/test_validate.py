"""Unit tests for SQL validation node."""

from unittest.mock import patch

import pytest
from agent_core.nodes.validate import validate_sql_node
from langchain_core.messages import HumanMessage


@pytest.fixture
def base_state():
    """Create a base agent state for testing."""
    return {
        "messages": [HumanMessage(content="Show me all customers")],
        "schema_context": "Table: customers (id, name, email)",
        "table_names": ["customers"],
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

        with patch("mlflow.start_span") as mock_span:
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

        with patch("mlflow.start_span") as mock_span:
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

        with patch("mlflow.start_span") as mock_span:
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
    async def test_no_sql_returns_error(self, base_state):
        """Test that missing SQL returns error."""
        base_state["current_sql"] = None

        with patch("mlflow.start_span") as mock_span:
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

        with patch("mlflow.start_span") as mock_span:
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

    @pytest.mark.asyncio
    async def test_metadata_preserved_on_failure(self, base_state):
        """Test that metadata is preserved even when validation fails."""
        base_state["current_sql"] = "SELECT * FROM payroll"

        with patch("mlflow.start_span") as mock_span:
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

        with patch("mlflow.start_span") as mock_span:
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
