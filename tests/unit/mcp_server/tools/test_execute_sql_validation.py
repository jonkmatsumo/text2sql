"""Tests for execute_sql_query validation hardening."""

import json

import pytest

from mcp_server.tools.execute_sql_query import handler


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
        assert "Only SELECT is allowed" in data["error"]["message"]

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
