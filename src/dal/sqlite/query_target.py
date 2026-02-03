import sqlite3
from contextlib import asynccontextmanager
from typing import Any, Dict, List, Optional

import aiosqlite

from dal.sqlite.param_translation import translate_postgres_params_to_sqlite
from dal.tracing import trace_query_operation
from dal.util.row_limits import cap_rows_with_metadata, get_sync_max_rows


class SqliteQueryTargetDatabase:
    """SQLite query-target database for local/dev use."""

    _db_path: Optional[str] = None
    _max_rows: int = 0

    @classmethod
    async def init(cls, db_path: Optional[str], max_rows: Optional[int] = None) -> None:
        """Initialize SQLite query-target config."""
        cls._db_path = db_path or ":memory:"
        cls._max_rows = max_rows if max_rows is not None else get_sync_max_rows()

    @classmethod
    async def close(cls) -> None:
        """Close SQLite resources (no-op for per-connection usage)."""
        return None

    @classmethod
    @asynccontextmanager
    async def get_connection(cls, tenant_id: Optional[int] = None, read_only: bool = False):
        """Yield a SQLite connection wrapper."""
        _ = tenant_id
        if cls._db_path is None:
            raise RuntimeError("SQLite DB path not configured. Set SQLITE_DB_PATH.")

        db_path, uri = _resolve_sqlite_path(cls._db_path, read_only)
        conn = await aiosqlite.connect(db_path, uri=uri, isolation_level=None)
        conn.row_factory = sqlite3.Row
        wrapper = _SqliteConnection(conn, max_rows=cls._max_rows)
        try:
            yield wrapper
        finally:
            await conn.close()


class _SqliteConnection:
    """Adapter providing asyncpg-like helpers over aiosqlite."""

    def __init__(self, conn: aiosqlite.Connection, max_rows: int) -> None:
        self._conn = conn
        self._max_rows = max_rows
        self._last_truncated = False

    @property
    def last_truncated(self) -> bool:
        """Return True when the last fetch was truncated by row limits."""
        return self._last_truncated

    async def execute(self, sql: str, *params: Any) -> str:
        sql, bound_params = translate_postgres_params_to_sqlite(sql, list(params))

        async def _run():
            cursor = await self._conn.execute(sql, bound_params)
            return _format_execute_status(sql, cursor.rowcount)

        return await trace_query_operation(
            "dal.query.execute",
            provider="sqlite",
            execution_model="sync",
            sql=sql,
            operation=_run(),
        )

    async def fetch(self, sql: str, *params: Any) -> List[Dict[str, Any]]:
        sql, bound_params = translate_postgres_params_to_sqlite(sql, list(params))

        async def _run():
            cursor = await self._conn.execute(sql, bound_params)
            rows = await cursor.fetchall()
            capped_rows, truncated = cap_rows_with_metadata(
                [dict(row) for row in rows], self._max_rows
            )
            self._last_truncated = truncated
            return capped_rows

        return await trace_query_operation(
            "dal.query.execute",
            provider="sqlite",
            execution_model="sync",
            sql=sql,
            operation=_run(),
        )

    async def fetchrow(self, sql: str, *params: Any) -> Optional[Dict[str, Any]]:
        rows = await self.fetch(sql, *params)
        return rows[0] if rows else None

    async def fetchval(self, sql: str, *params: Any) -> Any:
        row = await self.fetchrow(sql, *params)
        if row is None:
            return None
        return next(iter(row.values()))


def _resolve_sqlite_path(db_path: str, read_only: bool) -> tuple[str, bool]:
    if read_only and db_path not in (":memory:", ""):
        return f"file:{db_path}?mode=ro", True
    return db_path, False


def _format_execute_status(sql: str, rowcount: int) -> str:
    verb = sql.strip().split(maxsplit=1)
    if not verb:
        return "OK"
    op = verb[0].upper()
    if op in {"INSERT", "UPDATE", "DELETE"} and rowcount >= 0:
        return f"{op} {rowcount}"
    return "OK"
