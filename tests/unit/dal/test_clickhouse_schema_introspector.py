from contextlib import asynccontextmanager

import pytest

from dal.clickhouse.schema_introspector import ClickHouseSchemaIntrospector


class _FakeConn:
    def __init__(self, tables, columns):
        self._tables = tables
        self._columns = columns

    async def fetch(self, sql, *params):
        if "system.tables" in sql:
            return self._tables
        if "system.columns" in sql:
            return self._columns
        raise AssertionError(f"Unexpected SQL: {sql}")


@pytest.mark.asyncio
async def test_clickhouse_schema_introspector(monkeypatch):
    """Validate ClickHouse introspector mapping to canonical models."""

    class _FakeConfig:
        database = "analytics"

    tables = [{"name": "orders"}, {"name": "users"}]
    columns = [
        {"name": "id", "type": "Int64", "is_in_primary_key": 1},
        {"name": "user_id", "type": "Int64", "is_in_primary_key": 0},
    ]
    conn = _FakeConn(tables=tables, columns=columns)

    @asynccontextmanager
    async def fake_conn_ctx():
        yield conn

    monkeypatch.setattr("dal.clickhouse.schema_introspector.Database.get_connection", fake_conn_ctx)
    monkeypatch.setattr(
        "dal.clickhouse.schema_introspector.ClickHouseConfig.from_env", lambda: _FakeConfig()
    )

    introspector = ClickHouseSchemaIntrospector()
    table_names = await introspector.list_table_names()
    assert table_names == ["orders", "users"]

    table_def = await introspector.get_table_def("orders")
    assert [col.name for col in table_def.columns] == ["id", "user_id"]
