"""Regression tests for SQL read-only enforcement bypass vectors."""

from unittest.mock import AsyncMock, MagicMock, patch

import asyncpg
import pytest

import mcp_server.tools.execute_sql_query as execute_sql_query_mod

execute_sql_query = execute_sql_query_mod.handler


class TestSecurityRegression:
    """Tests identifying bypass vectors for the current regex-based security."""

    @pytest.mark.asyncio
    async def test_bypass_with_comment(self):
        """Test if regex handles keywords inside comments.

        Currently, the regex catches these, but we want to ensure any future change
        doesn't break this without moving to real DB-level enforcement.
        """
        # This SHOULD be blocked by current regex
        result = await execute_sql_query("SELECT 1; -- DROP TABLE users;", tenant_id=1)
        assert "Error:" in result
        assert "forbidden keyword" in result

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
            # We need to bypass regex for this test, so let's mock re.search to return None
            with patch("re.search", return_value=None):
                result = await execute_sql_query(bypass_query, tenant_id=1)

                assert "error" in result
                assert "cannot execute INSERT" in result

    @pytest.mark.asyncio
    async def test_bypass_with_cte_mutation(self):
        """Demonstrate that CTE mutations ARE blocked (if keyword present).

        Ensure we don't regress on this while we're still using regex.
        """
        # DELETE is in the list, so this is caught
        query = "WITH t AS (DELETE FROM film RETURNING *) SELECT * FROM t;"
        result = await execute_sql_query(query, tenant_id=1)
        assert "Error:" in result
        assert "DELETE" in result
