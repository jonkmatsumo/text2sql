"""Unit tests for Database connection pool management."""

from unittest.mock import AsyncMock, MagicMock, patch

import asyncpg
import pytest

from dal.database import Database


class TestDatabase:
    """Unit tests for Database connection pool management."""

    @pytest.fixture(autouse=True)
    def reset_pool(self):
        """Reset the pool before and after each test."""
        Database._pool = None
        yield
        Database._pool = None

    @pytest.mark.asyncio
    async def test_init_success(self):
        """Test successful pool initialization with default values."""
        mock_pool = AsyncMock(spec=asyncpg.Pool)

        with patch("dal.database.asyncpg.create_pool", new_callable=AsyncMock) as mock_create:
            mock_create.return_value = mock_pool
            with patch(
                "common.config.env.os.getenv",
                side_effect=lambda key, default=None: default,
            ):
                await Database.init()

                assert Database._pool == mock_pool
                mock_create.assert_called_once()
                call_kwargs = mock_create.call_args[1]
                assert call_kwargs["min_size"] == 5
                assert call_kwargs["max_size"] == 20
                assert call_kwargs["command_timeout"] == 60
                assert "server_settings" in call_kwargs

    @pytest.mark.asyncio
    async def test_init_with_env_vars(self):
        """Test pool initialization with custom environment variables."""
        mock_pool = AsyncMock(spec=asyncpg.Pool)
        env_vars = {
            "DB_HOST": "test-host",
            "DB_PORT": "5433",
            "DB_NAME": "test_db",
            "DB_USER": "test_user",
            "DB_PASS": "test_pass",
        }

        with patch("dal.database.asyncpg.create_pool", new_callable=AsyncMock) as mock_create:
            mock_create.return_value = mock_pool
            with patch(
                "common.config.env.os.getenv",
                side_effect=lambda key, default=None: env_vars.get(key, default),
            ):
                await Database.init()

                assert Database._pool == mock_pool
                call_args = mock_create.call_args
                dsn = call_args[0][0]
                assert "test_user" in dsn
                assert "test_pass" in dsn
                assert "test-host" in dsn
                assert "5433" in dsn
                assert "test_db" in dsn

    @pytest.mark.asyncio
    async def test_init_connection_error(self):
        """Test that ConnectionError is raised when pool creation fails."""
        with patch("dal.database.asyncpg.create_pool", new_callable=AsyncMock) as mock_create:
            mock_create.side_effect = Exception("Database connection failed")
            with patch(
                "common.config.env.os.getenv",
                side_effect=lambda key, default=None: default,
            ):
                with pytest.raises(ConnectionError) as exc_info:
                    await Database.init()

                assert "Failed to initialize databases" in str(exc_info.value)
                assert Database._pool is None

    @pytest.mark.asyncio
    async def test_close_with_pool(self):
        """Test closing an initialized pool."""
        mock_pool = AsyncMock(spec=asyncpg.Pool)
        Database._pool = mock_pool

        await Database.close()

        mock_pool.close.assert_called_once()
        # Note: close() doesn't set _pool to None, it just closes it
        # The pool is set to None in the fixture cleanup

    @pytest.mark.asyncio
    async def test_close_without_pool(self):
        """Test closing when no pool exists (should not raise error)."""
        Database._pool = None

        # Should not raise an error
        await Database.close()

    @pytest.mark.asyncio
    async def test_get_connection_context_manager(self):
        """Test that get_connection works as async context manager."""
        mock_pool = AsyncMock(spec=asyncpg.Pool)
        mock_conn = AsyncMock()
        mock_transaction = AsyncMock()

        # Setup transaction context manager behavior
        mock_transaction.__aenter__ = AsyncMock(return_value=mock_transaction)
        mock_transaction.__aexit__ = AsyncMock(return_value=False)
        mock_conn.transaction = MagicMock(return_value=mock_transaction)

        # Setup acquire() to return an async context manager
        mock_acquire_cm = AsyncMock()
        mock_acquire_cm.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_acquire_cm.__aexit__ = AsyncMock(return_value=False)
        mock_pool.acquire = MagicMock(return_value=mock_acquire_cm)
        Database._pool = mock_pool

        mock_capabilities = MagicMock()
        mock_capabilities.supports_transactions = True

        with patch.object(
            Database, "get_query_target_capabilities", return_value=mock_capabilities
        ):
            async with Database.get_connection() as conn:
                # We check if it is either the mock_conn or a TracedAsyncpgConnection wrapping it
                from dal.tracing import TracedAsyncpgConnection

                if isinstance(conn, TracedAsyncpgConnection):
                    assert conn._conn == mock_conn
                else:
                    assert conn == mock_conn
                mock_pool.acquire.assert_called_once()
                mock_conn.transaction.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_connection_with_tenant_id(self):
        """Test that tenant context is set when tenant_id is provided."""
        mock_pool = AsyncMock(spec=asyncpg.Pool)
        mock_conn = AsyncMock()
        mock_transaction = AsyncMock()

        # Setup transaction context manager behavior
        mock_transaction.__aenter__ = AsyncMock(return_value=mock_transaction)
        mock_transaction.__aexit__ = AsyncMock(return_value=False)
        mock_conn.transaction = MagicMock(return_value=mock_transaction)
        mock_conn.execute = AsyncMock()

        # Setup acquire() to return an async context manager
        mock_acquire_cm = AsyncMock()
        mock_acquire_cm.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_acquire_cm.__aexit__ = AsyncMock(return_value=False)
        mock_pool.acquire = MagicMock(return_value=mock_acquire_cm)
        Database._pool = mock_pool

        tenant_id = 1
        mock_capabilities = MagicMock()
        mock_capabilities.supports_transactions = True

        with patch.object(
            Database, "get_query_target_capabilities", return_value=mock_capabilities
        ):
            async with Database.get_connection(tenant_id=tenant_id) as conn:
                from dal.tracing import TracedAsyncpgConnection

                if isinstance(conn, TracedAsyncpgConnection):
                    assert conn._conn == mock_conn
                else:
                    assert conn == mock_conn
                # Verify set_config was called with tenant_id
                mock_conn.execute.assert_called_once()
                call_args = mock_conn.execute.call_args
                assert "set_config" in call_args[0][0]
                assert str(tenant_id) in call_args[0]

    @pytest.mark.asyncio
    async def test_get_connection_not_initialized(self):
        """Test that RuntimeError is raised when pool is not initialized."""
        Database._pool = None

        with pytest.raises(RuntimeError) as exc_info:
            async with Database.get_connection():
                pass

        assert "Database pool not initialized" in str(exc_info.value)
