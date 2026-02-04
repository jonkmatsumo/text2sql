import asyncio
import logging
import time
from contextlib import asynccontextmanager
from typing import Any, Dict, List, Optional

import snowflake.connector

from dal.async_query_executor import QueryStatus
from dal.snowflake.config import SnowflakeConfig
from dal.snowflake.executor import SnowflakeAsyncQueryExecutor
from dal.snowflake.param_translation import translate_postgres_params_to_snowflake
from dal.tracing import trace_query_operation


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
    _query_timeout_seconds: int = 30
    _poll_interval_seconds: int = 1
    _max_rows: int = 1000
    _warn_after_seconds: int = 10

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
        cls._query_timeout_seconds = config.query_timeout_seconds
        cls._poll_interval_seconds = config.poll_interval_seconds
        cls._max_rows = config.max_rows
        cls._warn_after_seconds = config.warn_after_seconds

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
        wrapper = _SnowflakeConnection(
            conn,
            query_timeout_seconds=cls._query_timeout_seconds,
            poll_interval_seconds=cls._poll_interval_seconds,
            max_rows=cls._max_rows,
            warn_after_seconds=cls._warn_after_seconds,
        )
        try:
            yield wrapper
        finally:
            await asyncio.to_thread(conn.close)


class _SnowflakeConnection:
    """Adapter providing asyncpg-like helpers over Snowflake connector."""

    def __init__(
        self,
        conn: snowflake.connector.SnowflakeConnection,
        query_timeout_seconds: int,
        poll_interval_seconds: int,
        max_rows: int,
        warn_after_seconds: int,
    ) -> None:
        self._conn = conn
        self._query_timeout_seconds = query_timeout_seconds
        self._poll_interval_seconds = poll_interval_seconds
        self._max_rows = max_rows
        self._warn_after_seconds = warn_after_seconds
        self._executor = SnowflakeAsyncQueryExecutor(conn)

    @property
    def executor(self) -> SnowflakeAsyncQueryExecutor:
        """Expose the async query executor for job-style operations."""
        return self._executor

    async def execute(self, sql: str, *params: Any) -> str:
        sql, bound_params = translate_postgres_params_to_snowflake(sql, list(params))

        async def _run():
            return await asyncio.to_thread(_execute, self._conn, sql, bound_params)

        return await trace_query_operation(
            "dal.query.execute",
            provider="snowflake",
            execution_model="async",
            sql=sql,
            operation=_run(),
        )

    async def fetch(self, sql: str, *params: Any) -> List[Dict[str, Any]]:
        sql, bound_params = translate_postgres_params_to_snowflake(sql, list(params))
        return await trace_query_operation(
            "dal.query.execute",
            provider="snowflake",
            execution_model="async",
            sql=sql,
            operation=_fetch_with_guardrails(
                self._executor,
                sql,
                bound_params,
                query_timeout_seconds=self._query_timeout_seconds,
                poll_interval_seconds=self._poll_interval_seconds,
                max_rows=self._max_rows,
                warn_after_seconds=self._warn_after_seconds,
            ),
        )

    async def fetch_with_columns(self, sql: str, *params: Any) -> tuple[List[Dict[str, Any]], list]:
        """Fetch rows with column metadata when supported."""
        sql, bound_params = translate_postgres_params_to_snowflake(sql, list(params))
        return await trace_query_operation(
            "dal.query.execute",
            provider="snowflake",
            execution_model="async",
            sql=sql,
            operation=_fetch_with_guardrails_with_columns(
                self._executor,
                sql,
                bound_params,
                query_timeout_seconds=self._query_timeout_seconds,
                poll_interval_seconds=self._poll_interval_seconds,
                max_rows=self._max_rows,
                warn_after_seconds=self._warn_after_seconds,
            ),
        )

    async def fetchrow(self, sql: str, *params: Any) -> Optional[Dict[str, Any]]:
        rows = await self.fetch(sql, *params)
        return rows[0] if rows else None

    async def fetchval(self, sql: str, *params: Any) -> Any:
        row = await self.fetchrow(sql, *params)
        if row is None:
            return None
        return next(iter(row.values()))


def _connect(cls: type["SnowflakeQueryTargetDatabase"]) -> snowflake.connector.SnowflakeConnection:
    session_parameters = {}
    if cls._query_timeout_seconds:
        session_parameters["STATEMENT_TIMEOUT_IN_SECONDS"] = cls._query_timeout_seconds
    return snowflake.connector.connect(
        account=cls._account,
        user=cls._user,
        password=cls._password,
        warehouse=cls._warehouse,
        database=cls._database,
        schema=cls._schema,
        role=cls._role,
        authenticator=cls._authenticator,
        session_parameters=session_parameters or None,
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


async def _fetch_with_guardrails(
    executor: SnowflakeAsyncQueryExecutor,
    sql: str,
    params: Dict[str, Any],
    query_timeout_seconds: int,
    poll_interval_seconds: int,
    max_rows: int,
    warn_after_seconds: int,
) -> List[Dict[str, Any]]:
    logger = logging.getLogger(__name__)
    started_at = time.monotonic()
    job_id = await executor.submit(sql, params if params else None)
    try:
        while True:
            status = await executor.poll(job_id)
            if status == QueryStatus.SUCCEEDED:
                break
            if status == QueryStatus.CANCELLED:
                raise RuntimeError(f"Snowflake query {job_id} was cancelled.")
            if status == QueryStatus.FAILED:
                raise RuntimeError(f"Snowflake query {job_id} failed.")
            if time.monotonic() - started_at >= query_timeout_seconds:
                await executor.cancel(job_id)
                raise TimeoutError(
                    f"Snowflake query {job_id} exceeded {query_timeout_seconds}s timeout."
                )
            await asyncio.sleep(poll_interval_seconds)
    except Exception:
        raise

    max_rows_limit = max_rows if max_rows > 0 else None
    rows = await executor.fetch(job_id, max_rows=max_rows_limit)
    elapsed = time.monotonic() - started_at
    if elapsed >= warn_after_seconds:
        logger.warning(
            "Snowflake query %s took %.2fs (warn threshold=%ss).",
            job_id,
            elapsed,
            warn_after_seconds,
        )
    if max_rows_limit and len(rows) >= max_rows_limit:
        logger.warning(
            "Snowflake query %s hit max rows cap (%s). Consider adding LIMIT.",
            job_id,
            max_rows_limit,
        )
    return rows


async def _fetch_with_guardrails_with_columns(
    executor: SnowflakeAsyncQueryExecutor,
    sql: str,
    params: Dict[str, Any],
    query_timeout_seconds: int,
    poll_interval_seconds: int,
    max_rows: int,
    warn_after_seconds: int,
) -> tuple[List[Dict[str, Any]], list]:
    """Fetch rows and columns with guardrails."""
    logger = logging.getLogger(__name__)
    started_at = time.monotonic()
    job_id = await executor.submit(sql, params if params else None)
    try:
        while True:
            status = await executor.poll(job_id)
            if status == QueryStatus.SUCCEEDED:
                break
            if status == QueryStatus.CANCELLED:
                raise RuntimeError(f"Snowflake query {job_id} was cancelled.")
            if status == QueryStatus.FAILED:
                raise RuntimeError(f"Snowflake query {job_id} failed.")
            if time.monotonic() - started_at >= query_timeout_seconds:
                await executor.cancel(job_id)
                raise TimeoutError(
                    f"Snowflake query {job_id} exceeded {query_timeout_seconds}s timeout."
                )
            await asyncio.sleep(poll_interval_seconds)
        rows, columns = await executor.fetch_with_columns(job_id, max_rows=max_rows)
        elapsed = time.monotonic() - started_at
        if elapsed >= warn_after_seconds:
            logger.warning("Snowflake query %s took %.2fs.", job_id, elapsed)
        if max_rows and len(rows) >= max_rows:
            logger.warning("Snowflake query %s hit max rows cap (%s).", job_id, max_rows)
        return rows, columns
    finally:
        await executor.cancel(job_id)


def _format_execute_status(sql: str, rowcount: int) -> str:
    verb = sql.strip().split(maxsplit=1)
    if not verb:
        return "OK"
    op = verb[0].upper()
    if op in {"INSERT", "UPDATE", "DELETE"} and rowcount >= 0:
        return f"{op} {rowcount}"
    return "OK"
