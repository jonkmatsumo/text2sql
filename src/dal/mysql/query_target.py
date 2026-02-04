from contextlib import asynccontextmanager
from typing import Any, Dict, List, Optional

import aiomysql

from dal.mysql.param_translation import translate_postgres_params_to_mysql
from dal.mysql.quoting import translate_double_quotes_to_backticks
from dal.tracing import trace_query_operation
from dal.util.row_limits import cap_rows_with_metadata, get_sync_max_rows


class MysqlQueryTargetDatabase:
    """MySQL query-target database using aiomysql."""

    _host: Optional[str] = None
    _port: int = 3306
    _db_name: Optional[str] = None
    _user: Optional[str] = None
    _password: Optional[str] = None
    _pool: Optional[aiomysql.Pool] = None
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
        """Initialize MySQL query-target config."""
        cls._host = host
        cls._port = port
        cls._db_name = db_name
        cls._user = user
        cls._password = password
        cls._max_rows = max_rows if max_rows is not None else get_sync_max_rows()

        missing = [
            name
            for name, value in {
                "DB_HOST": host,
                "DB_NAME": db_name,
                "DB_USER": user,
            }.items()
            if not value
        ]
        if missing:
            missing_list = ", ".join(missing)
            raise ValueError(
                f"MySQL query target missing required config: {missing_list}. "
                "Set DB_HOST, DB_NAME, and DB_USER."
            )
        if cls._pool is None:
            cls._pool = await aiomysql.create_pool(
                host=cls._host,
                port=cls._port,
                user=cls._user,
                password=cls._password,
                db=cls._db_name,
                autocommit=True,
                cursorclass=aiomysql.DictCursor,
            )

    @classmethod
    async def close(cls) -> None:
        """Close MySQL resources."""
        if cls._pool is not None:
            cls._pool.close()
            await cls._pool.wait_closed()
            cls._pool = None

    @classmethod
    @asynccontextmanager
    async def get_connection(cls, tenant_id: Optional[int] = None, read_only: bool = False):
        """Yield a MySQL connection wrapper (tenant context is a no-op)."""
        _ = tenant_id
        _ = read_only
        if cls._pool is None:
            raise RuntimeError("MySQL pool not initialized. Call MysqlQueryTargetDatabase.init().")

        async with cls._pool.acquire() as conn:
            wrapper = _MysqlConnection(conn, max_rows=cls._max_rows)
            yield wrapper


class _MysqlConnection:
    """Adapter providing asyncpg-like helpers over aiomysql."""

    def __init__(self, conn: aiomysql.Connection, max_rows: int) -> None:
        self._conn = conn
        self._max_rows = max_rows
        self._last_truncated = False

    @property
    def last_truncated(self) -> bool:
        """Return True when the last fetch was truncated by row limits."""
        return self._last_truncated

    async def execute(self, sql: str, *params: Any) -> str:
        sql = translate_double_quotes_to_backticks(sql)
        sql, bound_params = translate_postgres_params_to_mysql(sql, list(params))

        async def _run():
            async with self._conn.cursor() as cursor:
                await cursor.execute(sql, bound_params)
                return _format_execute_status(sql, cursor.rowcount)

        return await trace_query_operation(
            "dal.query.execute",
            provider="mysql",
            execution_model="sync",
            sql=sql,
            operation=_run(),
        )

    async def fetch(self, sql: str, *params: Any) -> List[Dict[str, Any]]:
        sql = translate_double_quotes_to_backticks(sql)
        sql, bound_params = translate_postgres_params_to_mysql(sql, list(params))

        async def _run():
            async with self._conn.cursor() as cursor:
                await cursor.execute(sql, bound_params)
                rows = await cursor.fetchall()
                capped_rows, truncated = cap_rows_with_metadata(list(rows), self._max_rows)
                self._last_truncated = truncated
                return capped_rows

        return await trace_query_operation(
            "dal.query.execute",
            provider="mysql",
            execution_model="sync",
            sql=sql,
            operation=_run(),
        )

    async def fetch_with_columns(self, sql: str, *params: Any) -> tuple[List[Dict[str, Any]], list]:
        """Fetch rows with column metadata when supported."""
        sql = translate_double_quotes_to_backticks(sql)
        sql, bound_params = translate_postgres_params_to_mysql(sql, list(params))

        async def _run():
            from dal.util.column_metadata import columns_from_cursor_description

            async with self._conn.cursor() as cursor:
                await cursor.execute(sql, bound_params)
                rows = await cursor.fetchall()
                capped_rows, truncated = cap_rows_with_metadata(list(rows), self._max_rows)
                self._last_truncated = truncated
                columns = columns_from_cursor_description(cursor.description, provider="mysql")
                return capped_rows, columns

        return await trace_query_operation(
            "dal.query.execute",
            provider="mysql",
            execution_model="sync",
            sql=sql,
            operation=_run(),
        )

    async def fetchrow(self, sql: str, *params: Any) -> Optional[Dict[str, Any]]:
        sql = translate_double_quotes_to_backticks(sql)
        sql, bound_params = translate_postgres_params_to_mysql(sql, list(params))

        async def _run():
            async with self._conn.cursor() as cursor:
                await cursor.execute(sql, bound_params)
                self._last_truncated = False
                return await cursor.fetchone()

        return await trace_query_operation(
            "dal.query.execute",
            provider="mysql",
            execution_model="sync",
            sql=sql,
            operation=_run(),
        )

    async def fetchval(self, sql: str, *params: Any) -> Any:
        row = await self.fetchrow(sql, *params)
        if row is None:
            return None
        return next(iter(row.values()))


def _format_execute_status(sql: str, rowcount: int) -> str:
    verb = sql.strip().split(maxsplit=1)
    if not verb:
        return "OK"
    op = verb[0].upper()
    if op in {"INSERT", "UPDATE", "DELETE"} and rowcount >= 0:
        return f"{op} {rowcount}"
    return "OK"
