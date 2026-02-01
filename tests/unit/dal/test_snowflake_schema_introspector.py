from contextlib import asynccontextmanager

import pytest

from dal.snowflake.schema_introspector import SnowflakeSchemaIntrospector


class _FakeConn:
    def __init__(self, tables, columns, fks):
        self._tables = tables
        self._columns = columns
        self._fks = fks

    async def fetch(self, sql, *params):
        if "INFORMATION_SCHEMA.TABLES" in sql:
            return self._tables
        if "INFORMATION_SCHEMA.COLUMNS" in sql:
            return self._columns
        if "INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS" in sql:
            return self._fks
        raise AssertionError(f"Unexpected SQL: {sql}")


@pytest.mark.asyncio
async def test_snowflake_schema_introspector_table_defs(monkeypatch):
    """Validate Snowflake introspector mapping to canonical models."""
    monkeypatch.setenv("SNOWFLAKE_DATABASE", "ANALYTICS")
    monkeypatch.setenv("SNOWFLAKE_SCHEMA", "PUBLIC")

    tables = [{"TABLE_NAME": "ORDERS"}, {"TABLE_NAME": "USERS"}]
    columns = [
        {"COLUMN_NAME": "ID", "DATA_TYPE": "NUMBER", "IS_NULLABLE": "NO"},
        {"COLUMN_NAME": "USER_ID", "DATA_TYPE": "NUMBER", "IS_NULLABLE": "YES"},
    ]
    fks = [
        {
            "COLUMN_NAME": "USER_ID",
            "REFERENCED_TABLE_NAME": "USERS",
            "REFERENCED_COLUMN_NAME": "ID",
        }
    ]
    conn = _FakeConn(tables=tables, columns=columns, fks=fks)

    @asynccontextmanager
    async def fake_conn_ctx():
        yield conn

    monkeypatch.setattr("dal.snowflake.schema_introspector.Database.get_connection", fake_conn_ctx)

    introspector = SnowflakeSchemaIntrospector()
    table_names = await introspector.list_table_names()
    assert table_names == ["ORDERS", "USERS"]

    table_def = await introspector.get_table_def("ORDERS")
    assert [col.name for col in table_def.columns] == ["ID", "USER_ID"]
    assert [col.data_type for col in table_def.columns] == ["NUMBER", "NUMBER"]
    assert table_def.foreign_keys[0].foreign_table_name == "USERS"
