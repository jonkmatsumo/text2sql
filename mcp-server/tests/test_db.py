"""Unit tests for Database connection pool management."""

from unittest.mock import AsyncMock, patch

import asyncpg
import pytest
from mcp_server.db import Database


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

        with patch("mcp_server.db.asyncpg.create_pool", new_callable=AsyncMock) as mock_create:
            mock_create.return_value = mock_pool
            with patch("mcp_server.db.os.getenv", side_effect=lambda key, default=None: default):
                await Database.init()

                assert Database._pool == mock_pool
                mock_create.assert_called_once()
                call_kwargs = mock_create.call_args[1]
                assert call_kwargs["min_size"] == 2
                assert call_kwargs["max_size"] == 10
                assert call_kwargs["command_timeout"] == 60

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

        with patch("mcp_server.db.asyncpg.create_pool", new_callable=AsyncMock) as mock_create:
            mock_create.return_value = mock_pool
            with patch(
                "mcp_server.db.os.getenv",
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
        with patch("mcp_server.db.asyncpg.create_pool", new_callable=AsyncMock) as mock_create:
            mock_create.side_effect = Exception("Connection refused")
            with patch("mcp_server.db.os.getenv", side_effect=lambda key, default=None: default):
                with pytest.raises(ConnectionError) as exc_info:
                    await Database.init()

                assert "Failed to create connection pool" in str(exc_info.value)
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
    async def test_get_connection_success(self):
        """Test successfully acquiring a connection from the pool."""
        mock_pool = AsyncMock(spec=asyncpg.Pool)
        mock_conn = AsyncMock()
        mock_pool.acquire = AsyncMock(return_value=mock_conn)
        Database._pool = mock_pool

        conn = await Database.get_connection()

        assert conn == mock_conn
        mock_pool.acquire.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_connection_not_initialized(self):
        """Test that RuntimeError is raised when pool is not initialized."""
        Database._pool = None

        with pytest.raises(RuntimeError) as exc_info:
            await Database.get_connection()

        assert "Database pool not initialized" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_release_connection_success(self):
        """Test releasing a connection back to the pool."""
        mock_pool = AsyncMock(spec=asyncpg.Pool)
        mock_conn = AsyncMock()
        Database._pool = mock_pool

        await Database.release_connection(mock_conn)

        mock_pool.release.assert_called_once_with(mock_conn)

    @pytest.mark.asyncio
    async def test_release_connection_no_pool(self):
        """Test releasing when no pool exists (should not raise error)."""
        Database._pool = None
        mock_conn = AsyncMock()

        # Should not raise an error
        await Database.release_connection(mock_conn)
