from contextlib import asynccontextmanager
from typing import Any, Dict, List, Optional

import asyncpg

from dal.tracing import trace_query_operation
from dal.util.row_limits import cap_rows_with_metadata


class RedshiftQueryTargetDatabase:
    """Redshift query-target database using asyncpg."""

    _host: Optional[str] = None
    _port: int = 5439
    _db_name: Optional[str] = None
    _user: Optional[str] = None
    _password: Optional[str] = None
    _pool: Optional[asyncpg.Pool] = None
    _max_rows: int = 0

    @classmethod
    async def init(
        cls,
        host: Optional[str],
        port: int,
        db_name: Optional[str],
        user: Optional[str],
        password: Optional[str],
        max_rows: Optional[int] = None,
    ) -> None:
        """Initialize Redshift query-target config."""
        cls._host = host
        cls._port = port
        cls._db_name = db_name
        cls._user = user
        cls._password = password
        cls._max_rows = max_rows or 0

        missing = [
            name
            for name, value in {
                "DB_HOST": host,
                "DB_NAME": db_name,
                "DB_USER": user,
                "DB_PASS": password,
            }.items()
            if not value
        ]
        if missing:
            missing_list = ", ".join(missing)
            raise ValueError(
                f"Redshift query target missing required config: {missing_list}. "
                "Set DB_HOST, DB_NAME, DB_USER, and DB_PASS."
            )
        if cls._pool is None:
            dsn = f"postgresql://{cls._user}:{cls._password}@{cls._host}:{cls._port}/{cls._db_name}"
            cls._pool = await asyncpg.create_pool(
                dsn,
                min_size=1,
                max_size=10,
                command_timeout=60,
                server_settings={"application_name": "bi_agent_redshift"},
            )

    @classmethod
    async def close(cls) -> None:
        """Close Redshift resources."""
        if cls._pool is not None:
            await cls._pool.close()
            cls._pool = None

    @classmethod
    @asynccontextmanager
    async def get_connection(cls, tenant_id: Optional[int] = None, read_only: bool = False):
        """Yield a Redshift connection wrapper (tenant context is a no-op)."""
        _ = tenant_id
        _ = read_only
        if cls._pool is None:
            raise RuntimeError(
                "Redshift pool not initialized. Call RedshiftQueryTargetDatabase.init()."
            )

        async with cls._pool.acquire() as conn:
            yield _RedshiftConnection(conn, max_rows=cls._max_rows)


class _RedshiftConnection:
    """Adapter providing asyncpg-like helpers over Redshift."""

    def __init__(self, conn: asyncpg.Connection, max_rows: int) -> None:
        self._conn = conn
        self._max_rows = max_rows
        self._last_truncated = False

    @property
    def last_truncated(self) -> bool:
        """Return True when the last fetch was truncated by row limits."""
        return self._last_truncated

    async def execute(self, sql: str, *params: Any) -> str:
        async def _run():
            return await self._conn.execute(sql, *params)

        return await trace_query_operation(
            "dal.query.execute",
            provider="redshift",
            execution_model="sync",
            sql=sql,
            operation=_run(),
        )

    async def fetch(self, sql: str, *params: Any) -> List[Dict[str, Any]]:
        async def _run():
            rows = await self._conn.fetch(sql, *params)
            capped_rows, truncated = cap_rows_with_metadata(
                [dict(row) for row in rows], self._max_rows
            )
            self._last_truncated = truncated
            return capped_rows

        return await trace_query_operation(
            "dal.query.execute",
            provider="redshift",
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
