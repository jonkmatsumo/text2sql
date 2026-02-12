"""Regression tests for SQL read-only enforcement bypass vectors."""

import json
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock, patch

import asyncpg
import pytest

import mcp_server.tools.execute_sql_query as execute_sql_query_mod

execute_sql_query = execute_sql_query_mod.handler


class TestSecurityRegression:
    """Tests identifying bypass vectors for the current regex-based security."""

    @pytest.fixture(autouse=True)
    def mock_security_checks(self):
        """Bypass role and policy checks for these low-level DB tests."""
        with (
            patch("mcp_server.utils.auth.validate_role", return_value=None),
            patch(
                "agent.validation.policy_enforcer.PolicyEnforcer.validate_sql", return_value=None
            ),
        ):
            yield

    @pytest.fixture(autouse=True)
    def mock_capabilities(self):
        """Mock Database capabilities."""
        mock_caps = MagicMock()
        mock_caps.supports_cancel = True
        mock_caps.supports_pagination = True
        mock_caps.supports_column_metadata = True
        mock_caps.execution_model = "async"

        with patch.object(
            execute_sql_query_mod.Database, "get_query_target_capabilities", return_value=mock_caps
        ):
            yield

    @pytest.mark.asyncio
    async def test_bypass_with_comment(self):
        """Commented trailing text should not be treated as a second statement."""

        class _Conn:
            async def fetch(self, _sql, *_params):
                return [{"value": 1}]

        @asynccontextmanager
        async def _conn_ctx(*_args, **_kwargs):
            yield _Conn()

        with patch.object(
            execute_sql_query_mod.Database, "get_connection", return_value=_conn_ctx()
        ):
            response_json = await execute_sql_query("SELECT 1; -- DROP TABLE users;", tenant_id=1)
            response = json.loads(response_json)

        assert "error" not in response
        assert response["rows"] == [{"value": 1}]

    @pytest.mark.asyncio
    async def test_bypass_with_set_blocked_by_db(self):
        """Verify that SET commands are passed to DB but should be read-only.

        With hardening, we rely on the DB role/session being read-only.
        """
        mock_conn = AsyncMock()
        mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_conn.__aexit__ = AsyncMock(return_value=False)

        # Simulate Postgres error for mutative SET if we were in a real DB,
        # but here we just check if read_only=True was passed to the context manager.
        mock_get = MagicMock()
        mock_get.return_value = mock_conn

        with patch.object(execute_sql_query_mod.Database, "get_connection", mock_get):
            # Bypass AST validation to test DB layer
            with patch("mcp_server.tools.execute_sql_query._validate_sql_ast", return_value=None):
                query = "SET session_replication_role = 'replica';"
                await execute_sql_query(query, tenant_id=1)

            # Verification: context manager called with read_only=True
            mock_get.assert_called_once_with(1, read_only=True)
            mock_conn.fetch.assert_called_once_with(query)

    @pytest.mark.asyncio
    async def test_db_enforcement_prevents_write(self):
        """Test that if regex is bypassed, DB error is returned."""
        # Suppose a new bypass is found that evades regex
        # e.g. a keyword split by comments if regex was weaker
        bypass_query = "SELECT 1; /* safe */ INSERT INTO film DEFAULT VALUES;"

        mock_conn = AsyncMock()
        # Simulate the error Postgres would throw in a READ ONLY transaction
        mock_conn.fetch.side_effect = asyncpg.ReadOnlySQLTransactionError(
            "cannot execute INSERT in a read-only transaction"
        )
        mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_conn.__aexit__ = AsyncMock(return_value=False)
        mock_get = MagicMock(return_value=mock_conn)

        with patch.object(execute_sql_query_mod.Database, "get_connection", mock_get):
            # Bypass AST validation to test DB layer
            with patch("mcp_server.tools.execute_sql_query._validate_sql_ast", return_value=None):
                response_json = await execute_sql_query(bypass_query, tenant_id=1)
                response = json.loads(response_json)

                assert "error" in response
                assert "cannot execute INSERT" in response["error"]["message"]

    @pytest.mark.asyncio
    async def test_bypass_with_cte_mutation(self):
        """Demonstrate that CTE mutations ARE blocked (if keyword present).

        Ensure we don't regress on this while we're still using regex.
        """
        # DELETE is in the list, so this is caught
        # DELETE in CTE.
        # This might pass AST if not recursively checked, but should fail at DB or be caught.
        # For now, we rely on DB read-only Check or AST.
        # Let's mock DB to ensure it's blocked there if AST passes, or expect AST error.
        # Actually, let's verify AST behavior.
        # If it passes AST, it fails at DB (Database not init).
        # We will assume AST is fine with it (as it is "read-only" tool, but deep
        # inspection is hard).
        # We'll rely on read-only transaction.

        mock_conn = AsyncMock()
        mock_conn.fetch.side_effect = asyncpg.ReadOnlySQLTransactionError(
            "cannot execute DELETE in a read-only transaction"
        )
        mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_conn.__aexit__ = AsyncMock(return_value=False)
        mock_get = MagicMock(return_value=mock_conn)

        with patch.object(execute_sql_query_mod.Database, "get_connection", mock_get):
            # We do NOT patch AST here. We want to see if it passes AST and hits DB,
            # OR is blocked by AST.
            query = "WITH t AS (DELETE FROM film RETURNING *) SELECT * FROM t;"
            response_json = await execute_sql_query(query, tenant_id=1)
            response = json.loads(response_json)

            assert "error" in response
            msg = response["error"]["message"]
            # Accepts either AST block or DB block
            assert "Forbidden" in msg or "cannot execute DELETE" in msg
