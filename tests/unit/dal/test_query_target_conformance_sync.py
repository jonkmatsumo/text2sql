"""Sync query-target conformance tests for local providers."""

import sqlite3

import pytest

from dal.sqlite import SqliteQueryTargetDatabase


@pytest.mark.asyncio
async def test_sqlite_query_target_conformance(tmp_path):
    """Validate SQLite query-target fetch/execute invariants."""
    db_path = tmp_path / "sqlite-conformance.db"
    await SqliteQueryTargetDatabase.init(str(db_path))

    async with SqliteQueryTargetDatabase.get_connection() as conn:
        await conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)")
        await conn.execute("INSERT INTO items (name) VALUES ($1)", "alpha")
        await conn.execute("INSERT INTO items (name) VALUES ($1)", "beta")

        rows = await conn.fetch("SELECT id, name FROM items ORDER BY id")
        assert rows == [{"id": 1, "name": "alpha"}, {"id": 2, "name": "beta"}]

        row = await conn.fetchrow("SELECT name FROM items WHERE id = $1", 2)
        assert row == {"name": "beta"}

        value = await conn.fetchval("SELECT name FROM items WHERE id = $1", 1)
        assert value == "alpha"

    async with SqliteQueryTargetDatabase.get_connection(read_only=True) as ro_conn:
        with pytest.raises(sqlite3.OperationalError):
            await ro_conn.execute("INSERT INTO items (name) VALUES ($1)", "gamma")


@pytest.mark.asyncio
async def test_duckdb_query_target_conformance(tmp_path):
    """Validate DuckDB query-target fetch/execute invariants."""
    duckdb = pytest.importorskip("duckdb")
    _ = duckdb

    from dal.duckdb import DuckDBConfig, DuckDBQueryTargetDatabase

    db_path = tmp_path / "duckdb-conformance.duckdb"
    await DuckDBQueryTargetDatabase.init(
        DuckDBConfig(path=str(db_path), query_timeout_seconds=5, max_rows=2)
    )

    async with DuckDBQueryTargetDatabase.get_connection() as conn:
        await conn.execute("CREATE TABLE items (id INTEGER, name TEXT)")
        await conn.execute("INSERT INTO items VALUES ($1, $2)", 1, "alpha")
        await conn.execute("INSERT INTO items VALUES ($1, $2)", 2, "beta")
        await conn.execute("INSERT INTO items VALUES ($1, $2)", 3, "gamma")

        rows = await conn.fetch("SELECT id, name FROM items ORDER BY id")
        assert rows == [{"id": 1, "name": "alpha"}, {"id": 2, "name": "beta"}]

        row = await conn.fetchrow("SELECT name FROM items WHERE id = $1", 2)
        assert row == {"name": "beta"}

        value = await conn.fetchval("SELECT name FROM items WHERE id = $1", 1)
        assert value == "alpha"
