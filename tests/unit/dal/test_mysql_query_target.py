from types import SimpleNamespace

import pytest

from dal.mysql.query_target import MysqlQueryTargetDatabase


@pytest.mark.asyncio
async def test_mysql_init_creates_pool(monkeypatch):
    """Ensure MySQL init wires a pool and close tears it down."""
    pool = SimpleNamespace(closed=False)

    async def fake_create_pool(**_kwargs):
        return pool

    def fake_close():
        pool.closed = True

    async def fake_wait_closed():
        return None

    pool.close = fake_close
    pool.wait_closed = fake_wait_closed

    monkeypatch.setattr("dal.mysql.query_target.aiomysql.create_pool", fake_create_pool)

    await MysqlQueryTargetDatabase.init(
        host="localhost",
        port=3306,
        db_name="query_target",
        user="user",
        password="pass",
    )
    assert MysqlQueryTargetDatabase._pool is pool

    await MysqlQueryTargetDatabase.close()
    assert MysqlQueryTargetDatabase._pool is None
