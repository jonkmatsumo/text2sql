"""Tests for PolicyEnforcer integration in execute_sql_query tool."""

import json
from unittest.mock import patch

import pytest

from agent.validation.policy_enforcer import PolicyEnforcer
from mcp_server.tools.execute_sql_query import handler


class TestMcpPolicyEnforcement:
    """Verify that PolicyEnforcer is called in the MCP tool path."""

    @pytest.fixture(autouse=True)
    def setup_policy(self):
        """Set up a controlled policy allowlist for testing and initialize Database."""
        from dal.capabilities import BackendCapabilities
        from dal.database import Database

        Database._query_target_capabilities = BackendCapabilities(
            supports_tenant_enforcement=True,
            tenant_enforcement_mode="rls_session",
            supports_column_metadata=True,
            supports_cancel=True,
            supports_pagination=True,
            execution_model="sync",
            supports_schema_cache=False,
        )
        Database._query_target_provider = "postgres"

        # Use a static allowlist to avoid DB introspection during tests
        PolicyEnforcer.set_allowed_tables({"users", "orders", "products"})
        yield
        # Reset after test
        PolicyEnforcer.set_allowed_tables(None)

    @pytest.mark.asyncio
    async def test_mcp_blocks_restricted_table(self):
        """Verify that forbidden tables are blocked in MCP path."""
        # Use a table that is NOT in the allowlist and NOT necessarily in the denylist
        # to ensure it's specifically hitting the PolicyEnforcer logic if needed.
        # Actually 'payroll' is in the denylist, so it hits _validate_sql_ast first.
        # To test PolicyEnforcer specifically, use a random table name.
        result = await handler("SELECT * FROM secret_internal_table", tenant_id=1)
        data = json.loads(result)

        assert "error" in data
        assert data["error"]["category"] == "invalid_request"
        # This one should specifically be the PolicyEnforcer message
        assert "Access to table <redacted_identifier> is not allowed" in data["error"]["message"]

    @pytest.mark.asyncio
    async def test_mcp_blocks_denylisted_table(self):
        """Verify that denylisted tables are blocked (either by denylist or enforcer)."""
        result = await handler("SELECT * FROM payroll", tenant_id=1)
        data = json.loads(result)

        assert "error" in data
        # Message might come from _validate_sql_ast (denylist)
        assert "Forbidden table: payroll is not allowed" in data["error"]["message"]

    @pytest.mark.asyncio
    async def test_mcp_blocks_sensitive_columns(self, monkeypatch):
        """Verify that PolicyEnforcer blocks sensitive columns in MCP path when enabled."""
        # Enable sensitive column blocking via env
        monkeypatch.setenv("AGENT_BLOCK_SENSITIVE_COLUMNS", "true")

        # 'password' is a sensitive column marker
        result = await handler("SELECT password FROM users", tenant_id=1)
        data = json.loads(result)

        assert "error" in data
        assert data["error"]["category"] == "invalid_request"
        assert "Sensitive column reference detected: password" in data["error"]["message"]

    @pytest.mark.asyncio
    async def test_mcp_allows_sensitive_columns_when_disabled(self, monkeypatch):
        """Verify that sensitive columns are allowed if the guardrail is off."""
        monkeypatch.setenv("AGENT_BLOCK_SENSITIVE_COLUMNS", "false")

        with patch("mcp_server.tools.execute_sql_query.Database.get_connection") as mock_conn:
            mock_conn.return_value.__aenter__.return_value.fetch.return_value = [
                {"password": "hashed"}
            ]

            result = await handler("SELECT password FROM users", tenant_id=1)
            data = json.loads(result)

            assert "error" not in data
            assert data["rows"] == [{"password": "hashed"}]
