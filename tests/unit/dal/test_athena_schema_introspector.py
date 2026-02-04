import sys
from contextlib import asynccontextmanager

import pytest

if sys.version_info < (3, 10):
    pytest.skip("Athena schema typing requires Python 3.10+", allow_module_level=True)

from dal.athena.schema_introspector import AthenaSchemaIntrospector


class _FakeConn:
    def __init__(self, tables, columns):
        self._tables = tables
        self._columns = columns

    async def fetch(self, sql, *params):
        if sql.startswith("SHOW TABLES"):
            return self._tables
        if sql.startswith("DESCRIBE"):
            return self._columns
        raise AssertionError(f"Unexpected SQL: {sql}")


@pytest.mark.asyncio
async def test_athena_schema_introspector(monkeypatch):
    """Validate Athena introspector mapping to canonical models."""

    class _FakeConfig:
        database = "analytics"

    tables = [{"tab_name": "orders"}, {"tab_name": "users"}]
    columns = [
        {"col_name": "id", "data_type": "int"},
        {"col_name": "user_id", "data_type": "int"},
        {"col_name": "# Partition Information", "data_type": ""},
    ]
    conn = _FakeConn(tables=tables, columns=columns)

    @asynccontextmanager
    async def fake_conn_ctx():
        yield conn

    monkeypatch.setattr("dal.athena.schema_introspector.Database.get_connection", fake_conn_ctx)
    monkeypatch.setattr(
        "dal.athena.schema_introspector.AthenaConfig.from_env", lambda: _FakeConfig()
    )

    introspector = AthenaSchemaIntrospector()
    table_names = await introspector.list_table_names()
    assert table_names == ["orders", "users"]

    table_def = await introspector.get_table_def("orders")
    assert [col.name for col in table_def.columns] == ["id", "user_id"]
