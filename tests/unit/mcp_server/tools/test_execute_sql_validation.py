"""Tests for execute_sql_query validation hardening."""

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from mcp_server.tools.execute_sql_query import _validate_sql_ast, handler


class TestExecuteSqlValidation:
    """Tests for execute_sql_query validation hardening."""

    def setup_method(self, method):
        """Initialize Database capabilities for tests."""
        from dal.capabilities import BackendCapabilities
        from dal.database import Database

        Database._query_target_capabilities = BackendCapabilities(
            supports_column_metadata=True,
            supports_cancel=True,
            supports_pagination=True,
            execution_model="sync",
            supports_schema_cache=False,
        )
        Database._query_target_provider = "postgres"

    @pytest.mark.asyncio
    async def test_validate_sql_multi_statement_complex(self):
        """Test rejecting complex multi-statement queries."""
        sql = "SELECT 1; SELECT 2"
        result = await handler(sql, tenant_id=1)
        data = json.loads(result)
        assert data["error"]["category"] == "invalid_request"
        assert "Multi-statement" in data["error"]["message"]

    @pytest.mark.asyncio
    async def test_validate_sql_non_select(self):
        """Test rejecting non-SELECT statements."""
        sql = "CREATE TABLE test (id INT)"
        result = await handler(sql, tenant_id=1)
        data = json.loads(result)
        assert data["error"]["category"] == "invalid_request"
        assert "Forbidden statement type" in data["error"]["message"]

    @pytest.mark.asyncio
    async def test_validate_params_not_list(self):
        """Test rejecting params that are not a list/tuple."""
        result = await handler("SELECT 1", tenant_id=1, params="not a list")
        data = json.loads(result)
        assert data["error"]["category"] == "invalid_request"
        assert "Parameters must be a list" in data["error"]["message"]

    @pytest.mark.asyncio
    async def test_validate_params_nested_list(self):
        """Test rejecting nested lists in params (unexpected structure)."""
        result = await handler("SELECT 1", tenant_id=1, params=[[1, 2]])
        data = json.loads(result)
        assert data["error"]["category"] == "invalid_request"
        assert "unsupported type" in data["error"]["message"]

    @pytest.mark.asyncio
    async def test_validate_params_unsupported_dict(self):
        """Test rejecting unsupported dict shapes in params."""
        result = await handler("SELECT 1", tenant_id=1, params=[{"unsupported": "shape"}])
        data = json.loads(result)
        assert data["error"]["category"] == "invalid_request"
        assert "unsupported type" in data["error"]["message"]

    @pytest.mark.asyncio
    async def test_validate_sql_blocks_restricted_table(self):
        """Direct MCP execution should reject restricted tables."""
        result = await handler("SELECT * FROM payroll", tenant_id=1)
        data = json.loads(result)
        assert data["error"]["category"] == "invalid_request"
        assert "Forbidden table" in data["error"]["message"]

    @pytest.mark.asyncio
    async def test_validate_sql_blocks_restricted_schema(self):
        """Direct MCP execution should reject restricted schemas."""
        result = await handler("SELECT table_name FROM information_schema.tables", tenant_id=1)
        data = json.loads(result)
        assert data["error"]["category"] == "invalid_request"
        assert "Forbidden schema/table reference" in data["error"]["message"]

    def test_validate_sql_ignores_block_markers_in_comments(self):
        """Comment markers should not trigger policy blocks in MCP validator."""
        sql = """
        -- payroll pg_sleep in comments should be ignored
        SELECT 1
        /* information_schema.tables */
        """
        assert _validate_sql_ast(sql, "postgres") is None

    def test_validate_sql_blocks_actual_restricted_table_with_comments(self):
        """Actual blocked references should still fail after stripping comments."""
        sql = """
        /* harmless */
        SELECT * FROM payroll
        -- users
        """
        error = _validate_sql_ast(sql, "postgres")
        assert isinstance(error, str)
        assert "Forbidden table" in error

    @pytest.mark.asyncio
    async def test_validate_sql_rejects_join_explosion(self, monkeypatch):
        """Queries with too many joins should fail complexity guardrails."""
        monkeypatch.setenv("MCP_MAX_JOINS", "2")
        sql = """
        SELECT *
        FROM t1
        JOIN t2 ON t1.id = t2.id
        JOIN t3 ON t2.id = t3.id
        JOIN t4 ON t3.id = t4.id
        """
        with (
            patch("mcp_server.utils.auth.validate_role", return_value=None),
            patch("agent.validation.policy_enforcer.PolicyEnforcer.validate_sql"),
        ):
            result = await handler(sql, tenant_id=1)
        data = json.loads(result)
        assert data["error"]["category"] == "invalid_request"
        assert "join count exceeds the allowed limit" in data["error"]["message"]
        assert data["error"]["details_safe"]["complexity_limit_name"] == "joins"
        assert data["error"]["details_safe"]["joins"] == 3

    @pytest.mark.asyncio
    async def test_validate_sql_rejects_cte_count_limit(self, monkeypatch):
        """Queries with too many CTEs should fail complexity guardrails."""
        monkeypatch.setenv("MCP_MAX_CTES", "2")
        sql = """
        WITH a AS (SELECT 1), b AS (SELECT 2), c AS (SELECT 3)
        SELECT * FROM a
        """
        with (
            patch("mcp_server.utils.auth.validate_role", return_value=None),
            patch("agent.validation.policy_enforcer.PolicyEnforcer.validate_sql"),
        ):
            result = await handler(sql, tenant_id=1)
        data = json.loads(result)
        assert data["error"]["category"] == "invalid_request"
        assert "CTE count exceeds the allowed limit" in data["error"]["message"]
        assert data["error"]["details_safe"]["complexity_limit_name"] == "ctes"
        assert data["error"]["details_safe"]["ctes"] == 3

    @pytest.mark.asyncio
    async def test_validate_sql_rejects_deep_subquery_nesting(self, monkeypatch):
        """Queries with deep nested subqueries should fail complexity guardrails."""
        monkeypatch.setenv("MCP_MAX_SUBQUERY_DEPTH", "1")
        sql = """
        SELECT *
        FROM t1
        WHERE t1.id IN (
            SELECT t2.id
            FROM t2
            WHERE t2.id IN (SELECT t3.id FROM t3)
        )
        """
        with (
            patch("mcp_server.utils.auth.validate_role", return_value=None),
            patch("agent.validation.policy_enforcer.PolicyEnforcer.validate_sql"),
        ):
            result = await handler(sql, tenant_id=1)
        data = json.loads(result)
        assert data["error"]["category"] == "invalid_request"
        assert "subquery nesting depth exceeds the allowed limit" in data["error"]["message"]
        assert data["error"]["details_safe"]["complexity_limit_name"] == "subquery_depth"

    @pytest.mark.asyncio
    async def test_validate_sql_rejects_cartesian_join(self):
        """Cartesian joins should be rejected when guard is enabled."""
        sql = "SELECT * FROM a, b"
        mock_span = MagicMock()
        mock_span.is_recording.return_value = True
        with (
            patch("mcp_server.utils.auth.validate_role", return_value=None),
            patch("agent.validation.policy_enforcer.PolicyEnforcer.validate_sql"),
            patch(
                "mcp_server.tools.execute_sql_query.trace.get_current_span", return_value=mock_span
            ),
        ):
            result = await handler(sql, tenant_id=1)

        data = json.loads(result)
        assert data["error"]["category"] == "invalid_request"
        assert "cartesian joins are not allowed" in data["error"]["message"]
        assert data["error"]["details_safe"]["cartesian_join_detected"] is True

        attrs = {}
        for call in mock_span.set_attribute.call_args_list:
            key, value = call[0]
            attrs[key] = value
        assert "sql.complexity.score" in attrs
        assert attrs["sql.complexity.cartesian_join_detected"] is True
        assert attrs["sql.complexity.limit_exceeded"] is True

    @pytest.mark.asyncio
    async def test_validate_sql_normal_query_passes_complexity_guard(self):
        """Normal single-table SELECT should pass complexity checks."""
        mock_conn = AsyncMock()
        mock_conn.fetch = AsyncMock(return_value=[{"id": 1}])
        mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_conn.__aexit__ = AsyncMock(return_value=False)
        mock_get = MagicMock(return_value=mock_conn)
        with (
            patch("mcp_server.utils.auth.validate_role", return_value=None),
            patch("agent.validation.policy_enforcer.PolicyEnforcer.validate_sql"),
            patch("mcp_server.tools.execute_sql_query.Database.get_connection", mock_get),
        ):
            result = await handler("SELECT id FROM users", tenant_id=1)
        data = json.loads(result)
        assert "error" not in data or data["error"] is None
        assert data["rows"] == [{"id": 1}]
