"""Read-only conformance harness across query-target providers."""

from pathlib import Path
from typing import Awaitable, Callable

import pytest

ProviderRunner = Callable[[Path], Awaitable[BaseException]]

PROVIDER_SKIP_REASONS = {
    "postgres": "requires external Postgres service in CI",
    "mysql": "requires external MySQL service in CI",
    "snowflake": "requires Snowflake credentials and network access",
    "redshift": "requires Redshift credentials and network access",
    "bigquery": "requires GCP credentials and network access",
    "athena": "requires AWS credentials and network access",
    "databricks": "requires Databricks credentials and network access",
    "clickhouse": "requires external ClickHouse service in CI",
    "cockroachdb": "requires external CockroachDB service in CI",
}


def _classify_read_only_violation(exc: BaseException) -> str:
    text = str(exc).lower()
    if any(fragment in text for fragment in ("unauthorized", "not authorized", "access denied")):
        return "unauthorized"
    if "permission denied" in text:
        return "unauthorized"
    return "invalid_request"


async def _sqlite_read_only_violation(tmp_path: Path) -> BaseException:
    pytest.importorskip("aiosqlite", reason="sqlite harness requires aiosqlite")
    from dal.sqlite import SqliteQueryTargetDatabase

    db_path = tmp_path / "sqlite-read-only-harness.db"
    await SqliteQueryTargetDatabase.init(str(db_path))
    async with SqliteQueryTargetDatabase.get_connection(read_only=False) as conn:
        await conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)")
        await conn.execute("INSERT INTO items (name) VALUES ($1)", "seed")

    with pytest.raises(Exception) as exc_info:
        async with SqliteQueryTargetDatabase.get_connection(read_only=True) as ro_conn:
            await ro_conn.execute("INSERT INTO items (name) VALUES ($1)", "blocked")
    return exc_info.value


async def _duckdb_read_only_violation(tmp_path: Path) -> BaseException:
    pytest.importorskip("duckdb", reason="duckdb harness requires duckdb package")
    from dal.duckdb import DuckDBConfig, DuckDBQueryTargetDatabase

    db_path = tmp_path / "duckdb-read-only-harness.duckdb"
    await DuckDBQueryTargetDatabase.init(
        DuckDBConfig(path=str(db_path), query_timeout_seconds=5, max_rows=100, read_only=False)
    )
    async with DuckDBQueryTargetDatabase.get_connection(read_only=False) as conn:
        await conn.execute("CREATE TABLE items (id INTEGER, name TEXT)")
        await conn.execute("INSERT INTO items VALUES ($1, $2)", 1, "seed")

    with pytest.raises(Exception) as exc_info:
        async with DuckDBQueryTargetDatabase.get_connection(read_only=True) as ro_conn:
            await ro_conn.execute("INSERT INTO items VALUES ($1, $2)", 2, "blocked")
    return exc_info.value


PROVIDER_RUNNERS: dict[str, ProviderRunner] = {
    "sqlite": _sqlite_read_only_violation,
    "duckdb": _duckdb_read_only_violation,
}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "provider",
    [
        "sqlite",
        "duckdb",
        "postgres",
        "mysql",
        "snowflake",
        "redshift",
        "bigquery",
        "athena",
        "databricks",
        "clickhouse",
        "cockroachdb",
    ],
)
async def test_provider_read_only_conformance(provider: str, tmp_path: Path):
    """Ensure write attempts fail while provider connections are read-only."""
    runner = PROVIDER_RUNNERS.get(provider)
    if runner is None:
        pytest.skip(PROVIDER_SKIP_REASONS[provider])

    violation = await runner(tmp_path)
    category = _classify_read_only_violation(violation)
    assert category in {"invalid_request", "unauthorized"}
    assert str(violation).strip()
