import json
import sqlite3

import pytest

from dal.database import Database
from mcp_server.tools.execute_sql_query import handler as execute_sql_query


@pytest.mark.asyncio
async def test_sqlite_query_target_introspection_and_exec(tmp_path, monkeypatch):
    """Exercise introspection and parameterized execution via execute_sql_query."""
    db_path = tmp_path / "e2e.db"
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
    conn.execute("INSERT INTO users (id, name) VALUES (1, 'Ada'), (2, 'Bob')")
    conn.commit()
    conn.close()

    monkeypatch.setenv("QUERY_TARGET_PROVIDER", "sqlite")
    monkeypatch.setenv("SQLITE_DB_PATH", str(db_path))

    await Database.init()
    try:
        introspector = Database.get_schema_introspector()
        table_names = await introspector.list_table_names()
        assert table_names == ["users"]

        table_def = await introspector.get_table_def("users")
        assert [col.name for col in table_def.columns] == ["id", "name"]

        result_json = await execute_sql_query(
            "SELECT name FROM users WHERE id = $1", tenant_id=1, params=[1]
        )
        data = json.loads(result_json)
        assert data["rows"] == [{"name": "Ada"}]
    finally:
        await Database.close()
