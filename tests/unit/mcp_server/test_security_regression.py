"""Regression tests for SQL read-only enforcement bypass vectors."""

from unittest.mock import AsyncMock, MagicMock, patch

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
    async def test_bypass_with_set_unblocked(self):
        """Demonstrate that SET commands are NOT currently blocked.

        This represents a P0 risk as it allows altering session state.
        """
        # This is NOT in the forbidden_patterns list
        # We use a mock to see if it would reach the DB
        mock_conn = AsyncMock()
        mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_conn.__aexit__ = AsyncMock(return_value=False)
        mock_get = MagicMock(return_value=mock_conn)

        with patch.object(execute_sql_query_mod.Database, "get_connection", mock_get):
            # This query is DANGEROUS but currently NOT blocked by regex
            query = "SET session_replication_role = 'replica';"
            await execute_sql_query(query, tenant_id=1)

            # Verification: It reaches the database
            mock_conn.fetch.assert_called_once_with(query)

    @pytest.mark.asyncio
    async def test_bypass_with_copy_unblocked(self):
        """Demonstrate that COPY commands are NOT currently blocked.

        Allows data exfiltration or arbitrary file writes.
        """
        mock_conn = AsyncMock()
        mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_conn.__aexit__ = AsyncMock(return_value=False)
        mock_get = MagicMock(return_value=mock_conn)

        with patch.object(execute_sql_query_mod.Database, "get_connection", mock_get):
            query = "COPY (SELECT * FROM film) TO '/tmp/exfil.csv';"
            await execute_sql_query(query, tenant_id=1)

            # Verification: It reaches the database
            mock_conn.fetch.assert_called_once_with(query)

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
