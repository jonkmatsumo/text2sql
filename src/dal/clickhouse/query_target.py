import asyncio
from contextlib import asynccontextmanager
from typing import Any, Dict, List, Optional

from dal.clickhouse.config import ClickHouseConfig
from dal.clickhouse.param_translation import translate_postgres_params_to_clickhouse


class ClickHouseQueryTargetDatabase:
    """ClickHouse query-target database wrapper."""

    _config: Optional[ClickHouseConfig] = None

    @classmethod
    async def init(cls, config: ClickHouseConfig) -> None:
        """Initialize ClickHouse query-target config."""
        cls._config = config

    @classmethod
    async def close(cls) -> None:
        """Close ClickHouse resources (no-op)."""
        return None

    @classmethod
    @asynccontextmanager
    async def get_connection(cls, tenant_id: Optional[int] = None, read_only: bool = False):
        """Yield a ClickHouse connection wrapper (tenant context is a no-op)."""
        _ = tenant_id
        _ = read_only
        if cls._config is None:
            raise RuntimeError(
                "ClickHouse config not initialized. Call ClickHouseQueryTargetDatabase.init()."
            )

        from asynch import connect

        conn = await connect(
            host=cls._config.host,
            port=cls._config.port,
            database=cls._config.database,
            user=cls._config.user,
            password=cls._config.password,
            secure=cls._config.secure,
        )
        wrapper = _ClickHouseConnection(
            conn,
            query_timeout_seconds=cls._config.query_timeout_seconds,
            max_rows=cls._config.max_rows,
        )
        try:
            yield wrapper
        finally:
            await conn.close()


class _ClickHouseConnection:
    """Adapter providing asyncpg-like helpers over ClickHouse."""

    def __init__(self, conn, query_timeout_seconds: int, max_rows: int) -> None:
        self._conn = conn
        self._query_timeout_seconds = query_timeout_seconds
        self._max_rows = max_rows

    async def execute(self, sql: str, *params: Any) -> str:
        await self._run_query(sql, list(params))
        return "OK"

    async def fetch(self, sql: str, *params: Any) -> List[Dict[str, Any]]:
        rows = await self._run_query(sql, list(params))
        if self._max_rows and len(rows) > self._max_rows:
            rows = rows[: self._max_rows]
        return rows

    async def fetchrow(self, sql: str, *params: Any) -> Optional[Dict[str, Any]]:
        rows = await self.fetch(sql, *params)
        return rows[0] if rows else None

    async def fetchval(self, sql: str, *params: Any) -> Any:
        row = await self.fetchrow(sql, *params)
        if row is None:
            return None
        return next(iter(row.values()))

    async def _run_query(self, sql: str, params: List[Any]) -> List[Dict[str, Any]]:
        translated_sql, bound_params = translate_postgres_params_to_clickhouse(sql, params)

        async def _execute():
            rows, columns = await self._conn.fetch(
                translated_sql, bound_params, columnar=False, with_column_types=True
            )
            col_names = [col[0] for col in columns]
            return [dict(zip(col_names, row)) for row in rows]

        return await asyncio.wait_for(
            _execute(),
            timeout=self._query_timeout_seconds,
        )
