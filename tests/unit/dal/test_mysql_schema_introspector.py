from contextlib import asynccontextmanager

import pytest

from dal.mysql.schema_introspector import MysqlSchemaIntrospector


class _FakeConn:
    def __init__(self, tables, columns, fks):
        self._tables = tables
        self._columns = columns
        self._fks = fks

    async def fetch(self, sql, *params):
        if "information_schema.tables" in sql:
            return self._tables
        if "information_schema.columns" in sql:
            return self._columns
        if "information_schema.key_column_usage" in sql:
            return self._fks
        raise AssertionError(f"Unexpected SQL: {sql}")


@pytest.mark.asyncio
async def test_mysql_schema_introspector_table_defs(monkeypatch):
    """Validate MySQL introspector mapping to canonical models."""
    tables = [{"table_name": "orders"}, {"table_name": "users"}]
    columns = [
        {"column_name": "id", "data_type": "int", "is_nullable": "NO", "column_key": "PRI"},
        {"column_name": "user_id", "data_type": "int", "is_nullable": "NO", "column_key": ""},
    ]
    fks = [
        {
            "column_name": "user_id",
            "foreign_table_name": "users",
            "foreign_column_name": "id",
        }
    ]
    conn = _FakeConn(tables=tables, columns=columns, fks=fks)

    @asynccontextmanager
    async def fake_conn_ctx():
        yield conn

    monkeypatch.setattr("dal.mysql.schema_introspector.Database.get_connection", fake_conn_ctx)

    introspector = MysqlSchemaIntrospector()
    table_names = await introspector.list_table_names()
    assert table_names == ["orders", "users"]

    table_def = await introspector.get_table_def("orders")
    assert [col.name for col in table_def.columns] == ["id", "user_id"]
    assert table_def.foreign_keys[0].foreign_table_name == "users"
