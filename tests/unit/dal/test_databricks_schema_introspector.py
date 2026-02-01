from contextlib import asynccontextmanager

import pytest

from dal.databricks.schema_introspector import DatabricksSchemaIntrospector


class _FakeConn:
    def __init__(self, tables, columns):
        self._tables = tables
        self._columns = columns

    async def fetch(self, sql, *params):
        if "system.information_schema.tables" in sql:
            return self._tables
        if "system.information_schema.columns" in sql:
            return self._columns
        raise AssertionError(f"Unexpected SQL: {sql}")


@pytest.mark.asyncio
async def test_databricks_schema_introspector(monkeypatch):
    """Validate Databricks introspector mapping to canonical models."""

    class _FakeConfig:
        catalog = "main"
        schema = "public"

    tables = [{"table_name": "orders"}, {"table_name": "users"}]
    columns = [
        {"column_name": "id", "data_type": "int", "is_nullable": "NO"},
        {"column_name": "user_id", "data_type": "int", "is_nullable": "YES"},
    ]
    conn = _FakeConn(tables=tables, columns=columns)

    @asynccontextmanager
    async def fake_conn_ctx():
        yield conn

    monkeypatch.setattr("dal.databricks.schema_introspector.Database.get_connection", fake_conn_ctx)
    monkeypatch.setattr(
        "dal.databricks.schema_introspector.DatabricksConfig.from_env", lambda: _FakeConfig()
    )

    introspector = DatabricksSchemaIntrospector()
    table_names = await introspector.list_table_names()
    assert table_names == ["orders", "users"]

    table_def = await introspector.get_table_def("orders")
    assert [col.name for col in table_def.columns] == ["id", "user_id"]
