"""Unit tests for Redshift QueryTargetDatabase wrapper (asyncpg mocked)."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest


class TestRedshiftQueryTargetDatabase:
    """Test Redshift query target wrapper behavior."""

    @pytest.mark.asyncio
    async def test_init_missing_config_raises(self):
        """Verify init raises ValueError when required config is missing."""
        from dal.redshift.query_target import RedshiftQueryTargetDatabase

        with pytest.raises(ValueError, match="missing required config"):
            await RedshiftQueryTargetDatabase.init(
                host=None,
                port=5439,
                db_name="dev",
                user="awsuser",
                password="secret",
            )

    @pytest.mark.asyncio
    async def test_init_requires_password(self):
        """Verify init requires DB_PASS."""
        from dal.redshift.query_target import RedshiftQueryTargetDatabase

        with pytest.raises(ValueError, match="DB_PASS"):
            await RedshiftQueryTargetDatabase.init(
                host="redshift.example.com",
                port=5439,
                db_name="dev",
                user="awsuser",
                password=None,
            )

    @pytest.mark.asyncio
    async def test_get_connection_without_init_raises(self):
        """Verify get_connection raises if pool not initialized."""
        from dal.redshift.query_target import RedshiftQueryTargetDatabase

        # Reset class state
        RedshiftQueryTargetDatabase._pool = None

        with pytest.raises(RuntimeError, match="not initialized"):
            async with RedshiftQueryTargetDatabase.get_connection():
                pass

    @pytest.mark.asyncio
    async def test_init_creates_pool_with_correct_dsn(self):
        """Verify init creates asyncpg pool with correct DSN."""
        from dal.redshift.query_target import RedshiftQueryTargetDatabase

        # Reset class state
        RedshiftQueryTargetDatabase._pool = None

        mock_pool = AsyncMock()

        # create_pool is a coroutine, so we need AsyncMock as the mock itself
        with patch(
            "dal.redshift.query_target.asyncpg.create_pool", new=AsyncMock(return_value=mock_pool)
        ) as mock_create:
            await RedshiftQueryTargetDatabase.init(
                host="redshift-cluster.example.com",
                port=5439,
                db_name="analytics",
                user="reader",
                password="secret123",
            )

            mock_create.assert_called_once()
            call_args = mock_create.call_args
            dsn = call_args[0][0]

            assert "redshift-cluster.example.com" in dsn
            assert "5439" in dsn
            assert "analytics" in dsn
            assert "reader" in dsn
            assert call_args[1]["server_settings"]["application_name"] == "bi_agent_redshift"

        # Cleanup
        RedshiftQueryTargetDatabase._pool = None

    @pytest.mark.asyncio
    async def test_get_connection_acquires_from_pool(self):
        """Verify get_connection acquires connection from pool."""
        from dal.redshift.query_target import RedshiftQueryTargetDatabase

        # Setup mock pool and connection
        mock_conn = AsyncMock()

        mock_pool = AsyncMock()
        mock_pool.acquire = MagicMock(
            return_value=AsyncMock(
                __aenter__=AsyncMock(return_value=mock_conn), __aexit__=AsyncMock()
            )
        )

        RedshiftQueryTargetDatabase._pool = mock_pool

        async with RedshiftQueryTargetDatabase.get_connection() as conn:
            assert conn is not None
            assert conn._conn is mock_conn

        # Cleanup
        RedshiftQueryTargetDatabase._pool = None

    @pytest.mark.asyncio
    async def test_get_connection_does_not_start_transaction(self):
        """Verify Redshift connection does not enter an explicit transaction."""
        from dal.redshift.query_target import RedshiftQueryTargetDatabase

        mock_conn = AsyncMock()
        mock_conn.transaction = MagicMock()

        mock_pool = AsyncMock()
        mock_pool.acquire = MagicMock(
            return_value=AsyncMock(
                __aenter__=AsyncMock(return_value=mock_conn), __aexit__=AsyncMock()
            )
        )

        RedshiftQueryTargetDatabase._pool = mock_pool

        async with RedshiftQueryTargetDatabase.get_connection():
            pass

        mock_conn.transaction.assert_not_called()

        RedshiftQueryTargetDatabase._pool = None

    @pytest.mark.asyncio
    async def test_get_connection_read_only_sets_session_flag(self):
        """Read-only connections should set Redshift session read-only mode."""
        from dal.redshift.query_target import RedshiftQueryTargetDatabase

        mock_conn = AsyncMock()
        mock_pool = AsyncMock()
        mock_pool.acquire = MagicMock(
            return_value=AsyncMock(
                __aenter__=AsyncMock(return_value=mock_conn), __aexit__=AsyncMock()
            )
        )
        RedshiftQueryTargetDatabase._pool = mock_pool

        async with RedshiftQueryTargetDatabase.get_connection(read_only=True):
            pass

        mock_conn.execute.assert_any_call("SET default_transaction_read_only = on")
        RedshiftQueryTargetDatabase._pool = None

    @pytest.mark.asyncio
    async def test_close_closes_pool(self):
        """Verify close() properly closes the pool."""
        from dal.redshift.query_target import RedshiftQueryTargetDatabase

        mock_pool = AsyncMock()
        RedshiftQueryTargetDatabase._pool = mock_pool

        await RedshiftQueryTargetDatabase.close()

        mock_pool.close.assert_called_once()
        assert RedshiftQueryTargetDatabase._pool is None

    @pytest.mark.asyncio
    async def test_close_when_no_pool_is_noop(self):
        """Verify close() is safe to call when pool is None."""
        from dal.redshift.query_target import RedshiftQueryTargetDatabase

        RedshiftQueryTargetDatabase._pool = None
        await RedshiftQueryTargetDatabase.close()  # Should not raise

    @pytest.mark.asyncio
    async def test_default_port_is_5439(self):
        """Verify default Redshift port is 5439 (not 5432)."""
        from dal.redshift.query_target import RedshiftQueryTargetDatabase

        assert RedshiftQueryTargetDatabase._port == 5439
