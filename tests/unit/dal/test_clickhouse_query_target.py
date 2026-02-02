"""Unit tests for ClickHouse QueryTargetDatabase wrapper (asynch mocked)."""

import types
from unittest.mock import AsyncMock

import pytest


class TestClickHouseQueryTargetDatabase:
    """Test ClickHouse query target wrapper behavior."""

    @pytest.fixture
    def mock_asynch_module(self):
        """Create a mock asynch module for testing."""
        mock_conn = AsyncMock()
        mock_conn.fetch = AsyncMock(
            return_value=(
                [(1, "Alice"), (2, "Bob")],  # rows
                [("id", "Int32"), ("name", "String")],  # columns with types
            )
        )
        mock_conn.close = AsyncMock()

        async def mock_connect(**kwargs):
            return mock_conn

        mock_asynch = types.SimpleNamespace(connect=mock_connect)
        return mock_asynch, mock_conn

    @pytest.mark.asyncio
    async def test_init_stores_config(self):
        """Verify init stores the config correctly."""
        from dal.clickhouse.config import ClickHouseConfig
        from dal.clickhouse.query_target import ClickHouseQueryTargetDatabase

        config = ClickHouseConfig(
            host="localhost",
            port=9000,
            database="default",
            user="default",
            password="",
            secure=False,
            query_timeout_seconds=30,
            max_rows=1000,
        )

        await ClickHouseQueryTargetDatabase.init(config)

        assert ClickHouseQueryTargetDatabase._config == config

        # Cleanup
        ClickHouseQueryTargetDatabase._config = None

    @pytest.mark.asyncio
    async def test_get_connection_without_init_raises(self):
        """Verify get_connection raises if config not initialized."""
        from dal.clickhouse.query_target import ClickHouseQueryTargetDatabase

        ClickHouseQueryTargetDatabase._config = None

        with pytest.raises(RuntimeError, match="not initialized"):
            async with ClickHouseQueryTargetDatabase.get_connection():
                pass

    @pytest.mark.asyncio
    async def test_fetch_returns_list_of_dicts(self, mock_asynch_module, monkeypatch):
        """Verify fetch converts rows to list of dicts."""
        mock_asynch, mock_conn = mock_asynch_module

        monkeypatch.setitem(__import__("sys").modules, "asynch", mock_asynch)

        from dal.clickhouse.config import ClickHouseConfig
        from dal.clickhouse.query_target import ClickHouseQueryTargetDatabase

        config = ClickHouseConfig(
            host="localhost",
            port=9000,
            database="default",
            user="default",
            password="",
            secure=False,
            query_timeout_seconds=30,
            max_rows=1000,
        )

        await ClickHouseQueryTargetDatabase.init(config)

        async with ClickHouseQueryTargetDatabase.get_connection() as conn:
            rows = await conn.fetch("SELECT id, name FROM users")

        assert rows == [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]

        ClickHouseQueryTargetDatabase._config = None

    @pytest.mark.asyncio
    async def test_fetch_enforces_max_rows(self, mock_asynch_module, monkeypatch):
        """Verify fetch respects max_rows limit."""
        mock_asynch, mock_conn = mock_asynch_module

        # Return more rows than max_rows
        mock_conn.fetch = AsyncMock(
            return_value=(
                [(i, f"User{i}") for i in range(100)],
                [("id", "Int32"), ("name", "String")],
            )
        )

        monkeypatch.setitem(__import__("sys").modules, "asynch", mock_asynch)

        from dal.clickhouse.config import ClickHouseConfig
        from dal.clickhouse.query_target import ClickHouseQueryTargetDatabase

        config = ClickHouseConfig(
            host="localhost",
            port=9000,
            database="default",
            user="default",
            password="",
            secure=False,
            query_timeout_seconds=30,
            max_rows=10,  # Only allow 10 rows
        )

        await ClickHouseQueryTargetDatabase.init(config)

        async with ClickHouseQueryTargetDatabase.get_connection() as conn:
            rows = await conn.fetch("SELECT id, name FROM users")

        assert len(rows) == 10  # Truncated to max_rows

        ClickHouseQueryTargetDatabase._config = None

    @pytest.mark.asyncio
    async def test_fetchrow_returns_single_dict(self, mock_asynch_module, monkeypatch):
        """Verify fetchrow returns first row as dict or None."""
        mock_asynch, mock_conn = mock_asynch_module

        monkeypatch.setitem(__import__("sys").modules, "asynch", mock_asynch)

        from dal.clickhouse.config import ClickHouseConfig
        from dal.clickhouse.query_target import ClickHouseQueryTargetDatabase

        config = ClickHouseConfig(
            host="localhost",
            port=9000,
            database="default",
            user="default",
            password="",
            secure=False,
            query_timeout_seconds=30,
            max_rows=1000,
        )

        await ClickHouseQueryTargetDatabase.init(config)

        async with ClickHouseQueryTargetDatabase.get_connection() as conn:
            row = await conn.fetchrow("SELECT id, name FROM users LIMIT 1")

        assert row == {"id": 1, "name": "Alice"}

        ClickHouseQueryTargetDatabase._config = None

    @pytest.mark.asyncio
    async def test_fetchval_returns_single_value(self, mock_asynch_module, monkeypatch):
        """Verify fetchval returns first column of first row."""
        mock_asynch, mock_conn = mock_asynch_module

        mock_conn.fetch = AsyncMock(
            return_value=(
                [(42,)],
                [("count", "Int64")],
            )
        )

        monkeypatch.setitem(__import__("sys").modules, "asynch", mock_asynch)

        from dal.clickhouse.config import ClickHouseConfig
        from dal.clickhouse.query_target import ClickHouseQueryTargetDatabase

        config = ClickHouseConfig(
            host="localhost",
            port=9000,
            database="default",
            user="default",
            password="",
            secure=False,
            query_timeout_seconds=30,
            max_rows=1000,
        )

        await ClickHouseQueryTargetDatabase.init(config)

        async with ClickHouseQueryTargetDatabase.get_connection() as conn:
            val = await conn.fetchval("SELECT count(*) FROM users")

        assert val == 42

        ClickHouseQueryTargetDatabase._config = None

    @pytest.mark.asyncio
    async def test_execute_returns_ok(self, mock_asynch_module, monkeypatch):
        """Verify execute returns OK status string."""
        mock_asynch, mock_conn = mock_asynch_module

        mock_conn.fetch = AsyncMock(return_value=([], []))

        monkeypatch.setitem(__import__("sys").modules, "asynch", mock_asynch)

        from dal.clickhouse.config import ClickHouseConfig
        from dal.clickhouse.query_target import ClickHouseQueryTargetDatabase

        config = ClickHouseConfig(
            host="localhost",
            port=9000,
            database="default",
            user="default",
            password="",
            secure=False,
            query_timeout_seconds=30,
            max_rows=1000,
        )

        await ClickHouseQueryTargetDatabase.init(config)

        async with ClickHouseQueryTargetDatabase.get_connection() as conn:
            status = await conn.execute("INSERT INTO users VALUES (3, 'Charlie')")

        assert status == "OK"

        ClickHouseQueryTargetDatabase._config = None

    @pytest.mark.asyncio
    async def test_param_translation_applied(self, mock_asynch_module, monkeypatch):
        """Verify Postgres $N params are translated to ClickHouse format."""
        mock_asynch, mock_conn = mock_asynch_module

        captured_sql = None
        captured_params = None

        async def capturing_fetch(sql, params, **kwargs):
            nonlocal captured_sql, captured_params
            captured_sql = sql
            captured_params = params
            return ([], [])

        mock_conn.fetch = capturing_fetch

        monkeypatch.setitem(__import__("sys").modules, "asynch", mock_asynch)

        from dal.clickhouse.config import ClickHouseConfig
        from dal.clickhouse.query_target import ClickHouseQueryTargetDatabase

        config = ClickHouseConfig(
            host="localhost",
            port=9000,
            database="default",
            user="default",
            password="",
            secure=False,
            query_timeout_seconds=30,
            max_rows=1000,
        )

        await ClickHouseQueryTargetDatabase.init(config)

        async with ClickHouseQueryTargetDatabase.get_connection() as conn:
            await conn.fetch("SELECT * FROM users WHERE id = $1", 42)

        # Should have translated $1 to ClickHouse format {p1: Int64}
        assert "{p1:" in captured_sql
        assert "p1" in captured_params
        assert captured_params["p1"] == 42

        ClickHouseQueryTargetDatabase._config = None

    @pytest.mark.asyncio
    async def test_close_is_noop(self):
        """Verify close() is safe (no persistent pool for ClickHouse)."""
        from dal.clickhouse.query_target import ClickHouseQueryTargetDatabase

        await ClickHouseQueryTargetDatabase.close()  # Should not raise
