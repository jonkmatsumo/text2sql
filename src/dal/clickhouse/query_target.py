from contextlib import asynccontextmanager
from typing import Any, Dict, List, Optional

from dal.clickhouse.config import ClickHouseConfig
from dal.clickhouse.param_translation import translate_postgres_params_to_clickhouse
from dal.tracing import trace_query_operation
from dal.util.row_limits import cap_rows_with_metadata, get_sync_max_rows
from dal.util.timeouts import run_with_timeout


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
        sync_max_rows = get_sync_max_rows()
        wrapper = _ClickHouseConnection(
            conn,
            query_timeout_seconds=cls._config.query_timeout_seconds,
            max_rows=cls._config.max_rows,
            sync_max_rows=sync_max_rows,
            read_only=read_only,
        )
        try:
            yield wrapper
        finally:
            await conn.close()


class _ClickHouseConnection:
    """Adapter providing asyncpg-like helpers over ClickHouse."""

    def __init__(
        self,
        conn,
        query_timeout_seconds: int,
        max_rows: int,
        sync_max_rows: int,
        read_only: bool = False,
    ) -> None:
        self._conn = conn
        self._query_timeout_seconds = query_timeout_seconds
        self._max_rows = max_rows
        self._sync_max_rows = sync_max_rows
        self._read_only = read_only
        self._last_truncated = False
        self._last_truncated_reason: Optional[str] = None

    @property
    def last_truncated(self) -> bool:
        """Return True when the last fetch was truncated by row limits."""
        return self._last_truncated

    @property
    def last_truncated_reason(self) -> Optional[str]:
        """Return the reason when the last fetch was truncated."""
        return self._last_truncated_reason

    async def execute(self, sql: str, *params: Any) -> str:
        from dal.util.read_only import enforce_read_only_sql

        enforce_read_only_sql(sql, provider="clickhouse", read_only=self._read_only)

        async def _run():
            await self._run_query(sql, list(params))
            return "OK"

        return await trace_query_operation(
            "dal.query.execute",
            provider="clickhouse",
            execution_model="sync",
            sql=sql,
            operation=_run(),
        )

    async def fetch(self, sql: str, *params: Any) -> List[Dict[str, Any]]:
        from dal.util.read_only import enforce_read_only_sql

        enforce_read_only_sql(sql, provider="clickhouse", read_only=self._read_only)

        async def _run():
            rows = await self._run_query(sql, list(params))
            limit = self._max_rows
            if self._sync_max_rows:
                limit = min(limit, self._sync_max_rows) if limit else self._sync_max_rows
            capped_rows, truncated = cap_rows_with_metadata(rows, limit)
            self._last_truncated = truncated
            self._last_truncated_reason = "PROVIDER_CAP" if truncated else None
            return capped_rows

        return await trace_query_operation(
            "dal.query.execute",
            provider="clickhouse",
            execution_model="sync",
            sql=sql,
            operation=_run(),
        )

    async def fetch_with_columns(self, sql: str, *params: Any) -> tuple[List[Dict[str, Any]], list]:
        """Fetch rows with column metadata when supported."""
        from dal.util.read_only import enforce_read_only_sql

        enforce_read_only_sql(sql, provider="clickhouse", read_only=self._read_only)

        async def _run():
            rows, columns = await self._run_query_with_columns(sql, list(params))
            limit = self._max_rows
            if self._sync_max_rows:
                limit = min(limit, self._sync_max_rows) if limit else self._sync_max_rows
            capped_rows, truncated = cap_rows_with_metadata(rows, limit)
            self._last_truncated = truncated
            self._last_truncated_reason = "PROVIDER_CAP" if truncated else None
            return capped_rows, columns

        return await trace_query_operation(
            "dal.query.execute",
            provider="clickhouse",
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
        translated_sql, bound_params = translate_postgres_params_to_clickhouse(sql, params)

        async def _execute():
            rows, columns = await self._conn.fetch(
                translated_sql, bound_params, columnar=False, with_column_types=True
            )
            col_names = [col[0] for col in columns]
            return [dict(zip(col_names, row)) for row in rows]

        return await run_with_timeout(
            _execute,
            timeout_seconds=self._query_timeout_seconds,
            provider="clickhouse",
            operation_name="query.execute",
        )

    async def _run_query_with_columns(
        self, sql: str, params: List[Any]
    ) -> tuple[List[Dict[str, Any]], list]:
        translated_sql, bound_params = translate_postgres_params_to_clickhouse(sql, params)

        async def _execute():
            rows, columns = await self._conn.fetch(
                translated_sql, bound_params, columnar=False, with_column_types=True
            )
            col_names = [col[0] for col in columns]
            row_dicts = [dict(zip(col_names, row)) for row in rows]
            return row_dicts, _columns_from_clickhouse_types(columns)

        return await run_with_timeout(
            _execute,
            timeout_seconds=self._query_timeout_seconds,
            provider="clickhouse",
            operation_name="query.fetch_with_columns",
        )


def _columns_from_clickhouse_types(columns: list) -> list:
    """Build column metadata from ClickHouse column types."""
    from dal.util.column_metadata import build_column_meta
    from dal.util.logical_types import logical_type_from_db_type

    metadata = []
    for name, db_type in columns or []:
        logical_type = logical_type_from_db_type(db_type, provider="clickhouse")
        metadata.append(build_column_meta(name, logical_type, db_type=db_type))
    return metadata
