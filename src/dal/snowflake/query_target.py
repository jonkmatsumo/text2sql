import asyncio
from contextlib import asynccontextmanager
from typing import Any, Dict, List, Optional

import snowflake.connector

from dal.snowflake.config import SnowflakeConfig
from dal.snowflake.executor import SnowflakeAsyncQueryExecutor
from dal.snowflake.param_translation import translate_postgres_params_to_snowflake


class SnowflakeQueryTargetDatabase:
    """Snowflake query-target database connection wrapper."""

    _account: Optional[str] = None
    _user: Optional[str] = None
    _password: Optional[str] = None
    _warehouse: Optional[str] = None
    _database: Optional[str] = None
    _schema: Optional[str] = None
    _role: Optional[str] = None
    _authenticator: Optional[str] = None

    @classmethod
    async def init(cls, config: SnowflakeConfig) -> None:
        """Initialize Snowflake query-target config."""
        cls._account = config.account
        cls._user = config.user
        cls._password = config.password
        cls._warehouse = config.warehouse
        cls._database = config.database
        cls._schema = config.schema
        cls._role = config.role
        cls._authenticator = config.authenticator

    @classmethod
    async def close(cls) -> None:
        """Close Snowflake resources (connections are per-context)."""
        return None

    @classmethod
    @asynccontextmanager
    async def get_connection(cls, tenant_id: Optional[int] = None, read_only: bool = False):
        """Yield a Snowflake connection wrapper (tenant context is a no-op)."""
        _ = tenant_id
        _ = read_only
        conn = await asyncio.to_thread(_connect, cls)
        wrapper = _SnowflakeConnection(conn)
        try:
            yield wrapper
        finally:
            await asyncio.to_thread(conn.close)


class _SnowflakeConnection:
    """Adapter providing asyncpg-like helpers over Snowflake connector."""

    def __init__(self, conn: snowflake.connector.SnowflakeConnection) -> None:
        self._conn = conn
        self._executor = SnowflakeAsyncQueryExecutor(conn)

    @property
    def executor(self) -> SnowflakeAsyncQueryExecutor:
        """Expose the async query executor for job-style operations."""
        return self._executor

    async def execute(self, sql: str, *params: Any) -> str:
        sql, bound_params = translate_postgres_params_to_snowflake(sql, list(params))
        return await asyncio.to_thread(_execute, self._conn, sql, bound_params)

    async def fetch(self, sql: str, *params: Any) -> List[Dict[str, Any]]:
        sql, bound_params = translate_postgres_params_to_snowflake(sql, list(params))
        return await asyncio.to_thread(_fetch, self._conn, sql, bound_params)

    async def fetchrow(self, sql: str, *params: Any) -> Optional[Dict[str, Any]]:
        rows = await self.fetch(sql, *params)
        return rows[0] if rows else None

    async def fetchval(self, sql: str, *params: Any) -> Any:
        row = await self.fetchrow(sql, *params)
        if row is None:
            return None
        return next(iter(row.values()))


def _connect(cls: type["SnowflakeQueryTargetDatabase"]) -> snowflake.connector.SnowflakeConnection:
    return snowflake.connector.connect(
        account=cls._account,
        user=cls._user,
        password=cls._password,
        warehouse=cls._warehouse,
        database=cls._database,
        schema=cls._schema,
        role=cls._role,
        authenticator=cls._authenticator,
    )


def _execute(
    conn: snowflake.connector.SnowflakeConnection, sql: str, params: Dict[str, Any]
) -> str:
    with conn.cursor() as cursor:
        cursor.execute(sql, params or None)
        rowcount = cursor.rowcount if cursor.rowcount is not None else -1
    return _format_execute_status(sql, rowcount)


def _fetch(
    conn: snowflake.connector.SnowflakeConnection, sql: str, params: Dict[str, Any]
) -> List[Dict[str, Any]]:
    with conn.cursor(snowflake.connector.DictCursor) as cursor:
        cursor.execute(sql, params or None)
        rows = cursor.fetchall()
    return [dict(row) for row in rows]


def _format_execute_status(sql: str, rowcount: int) -> str:
    verb = sql.strip().split(maxsplit=1)
    if not verb:
        return "OK"
    op = verb[0].upper()
    if op in {"INSERT", "UPDATE", "DELETE"} and rowcount >= 0:
        return f"{op} {rowcount}"
    return "OK"
