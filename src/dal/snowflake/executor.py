import asyncio
from typing import Any, Dict, List, Optional

import snowflake.connector
from snowflake.connector.constants import QueryStatus

from dal.async_query_executor import AsyncQueryExecutor
from dal.async_query_executor import QueryStatus as NormalizedStatus
from dal.tracing import trace_query_operation


class SnowflakeAsyncQueryExecutor(AsyncQueryExecutor):
    """AsyncQueryExecutor backed by Snowflake query IDs."""

    def __init__(
        self, conn: snowflake.connector.SnowflakeConnection, read_only: bool = False
    ) -> None:
        """Initialize the executor with a Snowflake connection."""
        self._conn = conn
        self._read_only = read_only

    async def submit(self, sql: str, params: Optional[dict[str, Any] | List[Any]] = None) -> str:
        """Submit a query for asynchronous execution."""
        from dal.util.read_only import enforce_read_only_sql

        enforce_read_only_sql(sql, provider="snowflake", read_only=self._read_only)
        bound_params = params if params is not None else []
        return await trace_query_operation(
            "dal.query.submit",
            provider="snowflake",
            execution_model="async",
            sql=sql,
            operation=asyncio.to_thread(_submit, self._conn, sql, bound_params),
        )

    async def poll(self, job_id: str) -> NormalizedStatus:
        """Poll the status of a running query."""
        status = await trace_query_operation(
            "dal.query.poll",
            provider="snowflake",
            execution_model="async",
            sql=None,
            operation=asyncio.to_thread(self._conn.get_query_status, job_id),
        )
        return _map_status(status)

    async def fetch(self, job_id: str, max_rows: Optional[int] = None) -> List[Dict[str, Any]]:
        """Fetch results for a completed query."""
        return await trace_query_operation(
            "dal.query.fetch",
            provider="snowflake",
            execution_model="async",
            sql=None,
            operation=asyncio.to_thread(_fetch, self._conn, job_id, max_rows),
        )

    async def fetch_with_columns(
        self, job_id: str, max_rows: Optional[int] = None
    ) -> tuple[List[Dict[str, Any]], list]:
        """Fetch results and column metadata for a completed query."""
        return await trace_query_operation(
            "dal.query.fetch",
            provider="snowflake",
            execution_model="async",
            sql=None,
            operation=asyncio.to_thread(_fetch_with_columns, self._conn, job_id, max_rows),
        )

    async def cancel(self, job_id: str) -> None:
        """Cancel a running query."""
        await asyncio.to_thread(self._conn.cancel_query, job_id)


def _submit(
    conn: snowflake.connector.SnowflakeConnection, sql: str, params: dict[str, Any] | list
) -> str:
    with conn.cursor() as cursor:
        cursor.execute_async(sql, params)
        return cursor.sfqid


def _fetch(
    conn: snowflake.connector.SnowflakeConnection,
    job_id: str,
    max_rows: Optional[int],
) -> List[Dict[str, Any]]:
    cursor = conn.get_results_from_sfqid(job_id)
    columns = [desc[0] for desc in cursor.description] if cursor.description else []
    batch_size = min(1000, max_rows) if max_rows else 1000
    rows: List[tuple] = []
    while True:
        batch = cursor.fetchmany(batch_size)
        if not batch:
            break
        rows.extend(batch)
        if max_rows is not None and len(rows) >= max_rows:
            rows = rows[:max_rows]
            break
    return [dict(zip(columns, row)) for row in rows]


def _fetch_with_columns(
    conn: snowflake.connector.SnowflakeConnection,
    job_id: str,
    max_rows: Optional[int],
) -> tuple[List[Dict[str, Any]], list]:
    """Fetch Snowflake results along with column metadata."""
    cursor = conn.get_results_from_sfqid(job_id)
    columns = _columns_from_snowflake_description(cursor.description or [])
    batch_size = min(1000, max_rows) if max_rows else 1000
    rows: List[tuple] = []
    while True:
        batch = cursor.fetchmany(batch_size)
        if not batch:
            break
        rows.extend(batch)
        if max_rows is not None and len(rows) >= max_rows:
            rows = rows[:max_rows]
            break
    col_names = [col.get("name") for col in columns]
    return [dict(zip(col_names, row)) for row in rows], columns


def _columns_from_snowflake_description(description: list) -> list:
    """Build column metadata from Snowflake cursor description."""
    from dal.util.column_metadata import columns_from_cursor_description

    return columns_from_cursor_description(description, provider="snowflake")


def _map_status(status: QueryStatus) -> NormalizedStatus:
    if status == QueryStatus.SUCCESS:
        return NormalizedStatus.SUCCEEDED
    if status in {QueryStatus.FAILED_WITH_ERROR, QueryStatus.ABORTED}:
        return NormalizedStatus.FAILED
    if status == QueryStatus.CANCELLED:
        return NormalizedStatus.CANCELLED
    return NormalizedStatus.RUNNING
