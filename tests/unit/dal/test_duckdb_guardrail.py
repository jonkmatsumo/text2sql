import pytest

from dal.duckdb.config import DuckDBConfig
from dal.duckdb.query_target import DuckDBQueryTargetDatabase


@pytest.mark.asyncio
async def test_duckdb_default_readonly(tmp_path):
    """Verify that DuckDB config defaults to read_only=True and blocks writes."""
    pytest.importorskip("duckdb")
    db_path = str(tmp_path / "test.duckdb")

    # 1. Setup: Create DB and add data in RW mode
    init_config = DuckDBConfig(path=db_path, query_timeout_seconds=5, max_rows=100, read_only=False)
    await DuckDBQueryTargetDatabase.init(init_config)
    async with DuckDBQueryTargetDatabase.get_connection(read_only=False) as conn:
        await conn.execute("CREATE TABLE foo (id INT)")
        await conn.execute("INSERT INTO foo VALUES (1)")

    # 2. Verify Default is RO
    # We explicitly do NOT set read_only, relying on the default we just changed
    ro_config = DuckDBConfig(
        path=db_path,
        query_timeout_seconds=5,
        max_rows=100,
        # read_only defaults to True
    )
    assert ro_config.read_only is True

    await DuckDBQueryTargetDatabase.init(ro_config)

    # 3. Test Enforcement
    async with DuckDBQueryTargetDatabase.get_connection(read_only=True) as conn:
        with pytest.raises(PermissionError, match="Read-only enforcement"):
            await conn.execute("INSERT INTO foo VALUES (2)")

        # SELECT should work
        assert await conn.fetchval("SELECT count(*) FROM foo") == 1
