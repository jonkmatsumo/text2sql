import asyncio
from contextlib import asynccontextmanager
from typing import Any, Dict, List, Optional

from dal.duckdb.config import DuckDBConfig
from dal.tracing import trace_query_operation
from dal.util.row_limits import cap_rows_with_metadata, get_sync_max_rows


class DuckDBQueryTargetDatabase:
    """DuckDB query-target database wrapper."""

    _config: Optional[DuckDBConfig] = None

    @classmethod
    async def init(cls, config: DuckDBConfig) -> None:
        """Initialize DuckDB query-target config."""
        cls._config = config

    @classmethod
    async def close(cls) -> None:
        """Close DuckDB resources (no-op)."""
        return None

    @classmethod
    @asynccontextmanager
    async def get_connection(cls, tenant_id: Optional[int] = None, read_only: bool = False):
        """Yield a DuckDB connection wrapper (tenant context is a no-op)."""
        _ = tenant_id
        _ = read_only
        if cls._config is None:
            raise RuntimeError(
                "DuckDB config not initialized. Call DuckDBQueryTargetDatabase.init()."
            )

        import duckdb

        conn = duckdb.connect(cls._config.path, read_only=cls._config.read_only)
        sync_max_rows = get_sync_max_rows()
        wrapper = _DuckDBConnection(
            conn,
            query_timeout_seconds=cls._config.query_timeout_seconds,
            max_rows=cls._config.max_rows,
            sync_max_rows=sync_max_rows,
        )
        try:
            yield wrapper
        finally:
            await asyncio.to_thread(conn.close)


class _DuckDBConnection:
    """Adapter providing asyncpg-like helpers over DuckDB."""

    def __init__(self, conn, query_timeout_seconds: int, max_rows: int, sync_max_rows: int) -> None:
        self._conn = conn
        self._query_timeout_seconds = query_timeout_seconds
        self._max_rows = max_rows
        self._sync_max_rows = sync_max_rows
        self._last_truncated = False

    @property
    def last_truncated(self) -> bool:
        """Return True when the last fetch was truncated by row limits."""
        return self._last_truncated

    async def execute(self, sql: str, *params: Any) -> str:
        async def _run():
            await self._run_query(sql, list(params))
            return "OK"

        return await trace_query_operation(
            "dal.query.execute",
            provider="duckdb",
            execution_model="sync",
            sql=sql,
            operation=_run(),
        )

    async def fetch(self, sql: str, *params: Any) -> List[Dict[str, Any]]:
        async def _run():
            rows = await self._run_query(sql, list(params))
            limit = self._max_rows
            if self._sync_max_rows:
                limit = min(limit, self._sync_max_rows) if limit else self._sync_max_rows
            capped_rows, truncated = cap_rows_with_metadata(rows, limit)
            self._last_truncated = truncated
            return capped_rows

        return await trace_query_operation(
            "dal.query.execute",
            provider="duckdb",
            execution_model="sync",
            sql=sql,
            operation=_run(),
        )

    async def fetch_with_columns(self, sql: str, *params: Any) -> tuple[List[Dict[str, Any]], list]:
        """Fetch rows with column metadata when supported."""

        async def _run():
            rows, columns = await self._run_query_with_columns(sql, list(params))
            limit = self._max_rows
            if self._sync_max_rows:
                limit = min(limit, self._sync_max_rows) if limit else self._sync_max_rows
            capped_rows, truncated = cap_rows_with_metadata(rows, limit)
            self._last_truncated = truncated
            return capped_rows, columns

        return await trace_query_operation(
            "dal.query.execute",
            provider="duckdb",
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

    async def _run_query(self, sql: str, params: List[Any]) -> List[Dict[str, Any]]:
        def _execute():
            cursor = self._conn.execute(sql, params)
            cols = [desc[0] for desc in cursor.description] if cursor.description else []
            return [dict(zip(cols, row)) for row in cursor.fetchall()]

        return await asyncio.wait_for(
            asyncio.to_thread(_execute),
            timeout=self._query_timeout_seconds,
        )

    async def _run_query_with_columns(
        self, sql: str, params: List[Any]
    ) -> tuple[List[Dict[str, Any]], list]:
        def _execute():
            from dal.util.column_metadata import columns_from_cursor_description

            cursor = self._conn.execute(sql, params)
            cols = [desc[0] for desc in cursor.description] if cursor.description else []
            rows = [dict(zip(cols, row)) for row in cursor.fetchall()]
            columns = columns_from_cursor_description(cursor.description, provider="duckdb")
            return rows, columns

        return await asyncio.wait_for(
            asyncio.to_thread(_execute),
            timeout=self._query_timeout_seconds,
        )
