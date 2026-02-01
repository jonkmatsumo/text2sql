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
