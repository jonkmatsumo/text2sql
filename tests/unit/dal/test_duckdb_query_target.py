import pytest

from dal.database import Database
from dal.duckdb import DuckDBConfig, DuckDBQueryTargetDatabase


@pytest.mark.requires_db
@pytest.mark.asyncio
async def test_duckdb_query_target_introspection_and_exec(tmp_path, monkeypatch):
    """Exercise DuckDB introspection + parameterized execution via execute_sql_query."""
    pytest.importorskip("duckdb")
    db_path = tmp_path / "duck.db"
    config = DuckDBConfig(path=str(db_path), query_timeout_seconds=5, max_rows=1000)
    await DuckDBQueryTargetDatabase.init(config)

    async with DuckDBQueryTargetDatabase.get_connection() as conn:
        await conn.execute("CREATE TABLE users (id INTEGER, name VARCHAR)")
        await conn.execute("INSERT INTO users VALUES (1, 'Ada'), (2, 'Bob')")

    monkeypatch.setenv("QUERY_TARGET_BACKEND", "duckdb")
    monkeypatch.setenv("DUCKDB_PATH", str(db_path))
    await Database.init()
    try:
        introspector = Database.get_schema_introspector()
        table_names = await introspector.list_table_names()
        assert "users" in table_names

        async with Database.get_connection() as conn:
            rows = await conn.fetch("SELECT name FROM users WHERE id = $1", 1)
            assert rows == [{"name": "Ada"}]
    finally:
        await Database.close()
