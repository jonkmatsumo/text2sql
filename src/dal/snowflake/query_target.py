import asyncio
from contextlib import asynccontextmanager
from typing import Any, Dict, List, Optional

import snowflake.connector


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
    async def init(
        cls,
        account: Optional[str],
        user: Optional[str],
        password: Optional[str],
        warehouse: Optional[str],
        database: Optional[str],
        schema: Optional[str],
        role: Optional[str],
        authenticator: Optional[str],
    ) -> None:
        """Initialize Snowflake query-target config."""
        cls._account = account
        cls._user = user
        cls._password = password
        cls._warehouse = warehouse
        cls._database = database
        cls._schema = schema
        cls._role = role
        cls._authenticator = authenticator

        missing = [
            name
            for name, value in {
                "SNOWFLAKE_ACCOUNT": account,
                "SNOWFLAKE_USER": user,
                "SNOWFLAKE_WAREHOUSE": warehouse,
                "SNOWFLAKE_DATABASE": database,
                "SNOWFLAKE_SCHEMA": schema,
            }.items()
            if not value
        ]
        if missing:
            missing_list = ", ".join(missing)
            raise ValueError(
                f"Snowflake query target missing required config: {missing_list}. "
                "Set SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_WAREHOUSE, "
                "SNOWFLAKE_DATABASE, and SNOWFLAKE_SCHEMA."
            )

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

    async def execute(self, sql: str, *params: Any) -> str:
        return await asyncio.to_thread(_execute, self._conn, sql, params)

    async def fetch(self, sql: str, *params: Any) -> List[Dict[str, Any]]:
        return await asyncio.to_thread(_fetch, self._conn, sql, params)

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


def _execute(conn: snowflake.connector.SnowflakeConnection, sql: str, params: Any) -> str:
    with conn.cursor() as cursor:
        cursor.execute(sql, params)
        rowcount = cursor.rowcount if cursor.rowcount is not None else -1
    return _format_execute_status(sql, rowcount)


def _fetch(
    conn: snowflake.connector.SnowflakeConnection, sql: str, params: Any
) -> List[Dict[str, Any]]:
    with conn.cursor(snowflake.connector.DictCursor) as cursor:
        cursor.execute(sql, params)
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
