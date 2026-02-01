from contextlib import asynccontextmanager
from typing import Any, Dict, List, Optional

import aiomysql

from dal.mysql.param_translation import translate_postgres_params_to_mysql
from dal.mysql.quoting import translate_double_quotes_to_backticks


class MysqlQueryTargetDatabase:
    """MySQL query-target database using aiomysql."""

    _host: Optional[str] = None
    _port: int = 3306
    _db_name: Optional[str] = None
    _user: Optional[str] = None
    _password: Optional[str] = None
    _pool: Optional[aiomysql.Pool] = None

    @classmethod
    async def init(
        cls,
        host: Optional[str],
        port: int,
        db_name: Optional[str],
        user: Optional[str],
        password: Optional[str],
    ) -> None:
        """Initialize MySQL query-target config."""
        cls._host = host
        cls._port = port
        cls._db_name = db_name
        cls._user = user
        cls._password = password

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
    async def get_connection(cls, *_args, **_kwargs):
        """Yield a MySQL connection wrapper."""
        if cls._pool is None:
            raise RuntimeError("MySQL pool not initialized. Call MysqlQueryTargetDatabase.init().")

        async with cls._pool.acquire() as conn:
            wrapper = _MysqlConnection(conn)
            yield wrapper


class _MysqlConnection:
    """Adapter providing asyncpg-like helpers over aiomysql."""

    def __init__(self, conn: aiomysql.Connection) -> None:
        self._conn = conn

    async def execute(self, sql: str, *params: Any) -> str:
        async with self._conn.cursor() as cursor:
            sql = translate_double_quotes_to_backticks(sql)
            sql, bound_params = translate_postgres_params_to_mysql(sql, list(params))
            await cursor.execute(sql, bound_params)
            return _format_execute_status(sql, cursor.rowcount)

    async def fetch(self, sql: str, *params: Any) -> List[Dict[str, Any]]:
        async with self._conn.cursor() as cursor:
            sql = translate_double_quotes_to_backticks(sql)
            sql, bound_params = translate_postgres_params_to_mysql(sql, list(params))
            await cursor.execute(sql, bound_params)
            rows = await cursor.fetchall()
            return list(rows)

    async def fetchrow(self, sql: str, *params: Any) -> Optional[Dict[str, Any]]:
        async with self._conn.cursor() as cursor:
            sql = translate_double_quotes_to_backticks(sql)
            sql, bound_params = translate_postgres_params_to_mysql(sql, list(params))
            await cursor.execute(sql, bound_params)
            row = await cursor.fetchone()
            return row

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
