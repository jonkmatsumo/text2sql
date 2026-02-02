from contextlib import asynccontextmanager

import pytest

from dal.redshift.schema_introspector import RedshiftSchemaIntrospector


class _FakeConn:
    def __init__(self, tables, columns):
        self._tables = tables
        self._columns = columns

    async def fetch(self, sql, *params):
        if "information_schema.tables" in sql:
            return self._tables
        if "information_schema.columns" in sql:
            return self._columns
        raise AssertionError(f"Unexpected SQL: {sql}")


@pytest.mark.asyncio
async def test_redshift_schema_introspector_table_defs(monkeypatch):
    """Validate Redshift introspector mapping to canonical models."""
    tables = [{"table_name": "orders"}, {"table_name": "users"}]
    columns = [
        {"column_name": "id", "data_type": "integer", "is_nullable": "NO"},
        {"column_name": "user_id", "data_type": "integer", "is_nullable": "YES"},
    ]
    conn = _FakeConn(tables=tables, columns=columns)

    @asynccontextmanager
    async def fake_conn_ctx():
        yield conn

    monkeypatch.setattr("dal.redshift.schema_introspector.Database.get_connection", fake_conn_ctx)

    introspector = RedshiftSchemaIntrospector()
    table_names = await introspector.list_table_names()
    assert table_names == ["orders", "users"]

    table_def = await introspector.get_table_def("orders")
    assert [col.name for col in table_def.columns] == ["id", "user_id"]
    assert [col.data_type for col in table_def.columns] == ["integer", "integer"]
