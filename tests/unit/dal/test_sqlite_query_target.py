import pytest

from dal.sqlite.query_target import SqliteQueryTargetDatabase


@pytest.mark.asyncio
async def test_sqlite_query_target_select_one(tmp_path):
    """Ensure SQLite query target can execute a basic SELECT."""
    db_path = tmp_path / "query_target.db"
    await SqliteQueryTargetDatabase.init(str(db_path))

    async with SqliteQueryTargetDatabase.get_connection() as conn:
        rows = await conn.fetch("SELECT 1 AS value")

    assert rows == [{"value": 1}]


@pytest.mark.asyncio
async def test_sqlite_memory_database():
    """Verify :memory: SQLite database works for transient queries."""
    await SqliteQueryTargetDatabase.init(":memory:")

    async with SqliteQueryTargetDatabase.get_connection() as conn:
        # Create a table and insert data in the same connection
        await conn.execute("CREATE TABLE test_mem (id INTEGER PRIMARY KEY, name TEXT)")
        await conn.execute("INSERT INTO test_mem (id, name) VALUES ($1, $2)", 1, "Alice")
        rows = await conn.fetch("SELECT id, name FROM test_mem WHERE id = $1", 1)

    assert rows == [{"id": 1, "name": "Alice"}]


@pytest.mark.asyncio
async def test_sqlite_memory_with_introspection():
    """Verify schema introspection works on :memory: databases."""
    await SqliteQueryTargetDatabase.init(":memory:")

    async with SqliteQueryTargetDatabase.get_connection() as conn:
        # Create schema
        await conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
        await conn.execute(
            "CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER REFERENCES users(id))"
        )

        # Verify tables exist via sqlite_master
        rows = await conn.fetch(
            "SELECT name FROM sqlite_master "
            "WHERE type='table' AND name NOT LIKE 'sqlite_%' "
            "ORDER BY name"
        )

    assert len(rows) == 2
    assert rows[0]["name"] == "orders"
    assert rows[1]["name"] == "users"


@pytest.mark.asyncio
async def test_sqlite_fetchrow_returns_single_dict():
    """Verify fetchrow returns a single dict or None."""
    await SqliteQueryTargetDatabase.init(":memory:")

    async with SqliteQueryTargetDatabase.get_connection() as conn:
        await conn.execute("CREATE TABLE t (id INTEGER)")
        await conn.execute("INSERT INTO t (id) VALUES ($1)", 42)

        row = await conn.fetchrow("SELECT id FROM t WHERE id = $1", 42)
        assert row == {"id": 42}

        missing = await conn.fetchrow("SELECT id FROM t WHERE id = $1", 999)
        assert missing is None


@pytest.mark.asyncio
async def test_sqlite_fetchval_returns_single_value():
    """Verify fetchval returns a single scalar value."""
    await SqliteQueryTargetDatabase.init(":memory:")

    async with SqliteQueryTargetDatabase.get_connection() as conn:
        val = await conn.fetchval("SELECT 42 AS answer")
        assert val == 42

        await conn.execute("CREATE TABLE empty (id INTEGER)")
        missing_val = await conn.fetchval("SELECT id FROM empty")
        assert missing_val is None
