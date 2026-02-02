from contextlib import asynccontextmanager

import pytest

from dal.redshift.schema_introspector import RedshiftSchemaIntrospector


class _CapturingConn:
    def __init__(self):
        self.queries = []

    async def fetch(self, sql, *params):
        self.queries.append(sql)
        return []


@pytest.mark.asyncio
async def test_redshift_introspector_uses_information_schema(monkeypatch):
    """Ensure Redshift introspector queries information_schema views."""
    conn = _CapturingConn()

    @asynccontextmanager
    async def fake_conn_ctx():
        yield conn

    monkeypatch.setattr("dal.redshift.schema_introspector.Database.get_connection", fake_conn_ctx)

    introspector = RedshiftSchemaIntrospector()
    await introspector.list_table_names()
    await introspector.get_table_def("users")

    assert any("information_schema.tables" in q for q in conn.queries)
    assert any("information_schema.columns" in q for q in conn.queries)
