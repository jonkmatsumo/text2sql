import asyncio
import logging
import time
from contextlib import asynccontextmanager
from typing import Any, Dict, List, Optional

from dal.async_query_executor import QueryStatus
from dal.bigquery.config import BigQueryConfig
from dal.bigquery.executor import BigQueryAsyncQueryExecutor
from dal.bigquery.param_translation import translate_postgres_params_to_bigquery
from dal.tracing import trace_query_operation
from dal.util.read_only import enforce_read_only_sql


class BigQueryQueryTargetDatabase:
    """BigQuery query-target database wrapper."""

    supports_tenant_enforcement: bool = False
    _config: Optional[BigQueryConfig] = None

    @classmethod
    async def init(cls, config: BigQueryConfig) -> None:
        """Initialize BigQuery query-target config."""
        cls._config = config

    @classmethod
    async def close(cls) -> None:
        """Close BigQuery resources (no-op)."""
        return None

    @classmethod
    @asynccontextmanager
    async def get_connection(cls, tenant_id: Optional[int] = None, read_only: bool = False):
        """Yield a BigQuery connection wrapper (tenant context is a no-op).

        BigQuery jobs don't expose a session-level read-only switch, so
        statement-level read-only checks are enforced in the DAL wrapper.
        """
        _ = tenant_id
        if cls._config is None:
            raise RuntimeError(
                "BigQuery config not initialized. Call BigQueryQueryTargetDatabase.init()."
            )
        executor = BigQueryAsyncQueryExecutor(
            project=cls._config.project,
            location=cls._config.location,
            timeout_seconds=cls._config.query_timeout_seconds,
            max_rows=cls._config.max_rows,
            read_only=read_only,
        )
        wrapper = _BigQueryConnection(
            executor=executor,
            query_timeout_seconds=cls._config.query_timeout_seconds,
            poll_interval_seconds=cls._config.poll_interval_seconds,
            max_rows=cls._config.max_rows,
            read_only=read_only,
        )
        yield wrapper


class _BigQueryConnection:
    """Adapter providing asyncpg-like helpers over BigQuery executor."""

    def __init__(
        self,
        executor: BigQueryAsyncQueryExecutor,
        query_timeout_seconds: int,
        poll_interval_seconds: int,
        max_rows: int,
        read_only: bool,
    ) -> None:
        self._executor = executor
        self._query_timeout_seconds = query_timeout_seconds
        self._poll_interval_seconds = poll_interval_seconds
        self._max_rows = max_rows
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

    def _set_truncation(self, row_count: int) -> None:
        truncated = bool(self._max_rows and row_count >= self._max_rows)
        self._last_truncated = truncated
        self._last_truncated_reason = "PROVIDER_CAP" if truncated else None

    async def execute(self, sql: str, *params: Any) -> str:
        enforce_read_only_sql(sql, provider="bigquery", read_only=self._read_only)
        sql, query_params = translate_postgres_params_to_bigquery(sql, list(params))

        async def _run():
            job_id = await self._executor.submit(sql, query_params)
            await _poll_until_done(
                self._executor,
                job_id,
                query_timeout_seconds=self._query_timeout_seconds,
                poll_interval_seconds=self._poll_interval_seconds,
            )
            return "OK"

        return await trace_query_operation(
            "dal.query.execute",
            provider="bigquery",
            execution_model="async",
            sql=sql,
            operation=_run(),
        )

    async def fetch(self, sql: str, *params: Any) -> List[Dict[str, Any]]:
        enforce_read_only_sql(sql, provider="bigquery", read_only=self._read_only)
        sql, query_params = translate_postgres_params_to_bigquery(sql, list(params))
        rows = await trace_query_operation(
            "dal.query.execute",
            provider="bigquery",
            execution_model="async",
            sql=sql,
            operation=_fetch_with_guardrails(
                self._executor,
                sql,
                query_params,
                query_timeout_seconds=self._query_timeout_seconds,
                poll_interval_seconds=self._poll_interval_seconds,
                max_rows=self._max_rows,
            ),
        )
        self._set_truncation(len(rows))
        return rows

    async def fetch_with_columns(self, sql: str, *params: Any) -> tuple[List[Dict[str, Any]], list]:
        """Fetch rows with column metadata when supported."""
        enforce_read_only_sql(sql, provider="bigquery", read_only=self._read_only)
        sql, query_params = translate_postgres_params_to_bigquery(sql, list(params))
        rows, columns = await trace_query_operation(
            "dal.query.execute",
            provider="bigquery",
            execution_model="async",
            sql=sql,
            operation=_fetch_with_guardrails_with_columns(
                self._executor,
                sql,
                query_params,
                query_timeout_seconds=self._query_timeout_seconds,
                poll_interval_seconds=self._poll_interval_seconds,
                max_rows=self._max_rows,
            ),
        )
        self._set_truncation(len(rows))
        return rows, columns

    async def fetchrow(self, sql: str, *params: Any) -> Optional[Dict[str, Any]]:
        rows = await self.fetch(sql, *params)
        return rows[0] if rows else None

    async def fetchval(self, sql: str, *params: Any) -> Any:
        row = await self.fetchrow(sql, *params)
        if row is None:
            return None
        return next(iter(row.values()))


async def _poll_until_done(
    executor: BigQueryAsyncQueryExecutor,
    job_id: str,
    query_timeout_seconds: int,
    poll_interval_seconds: int,
) -> None:
    started_at = time.monotonic()
    while True:
        status = await executor.poll(job_id)
        if status == QueryStatus.SUCCEEDED:
            return
        if status == QueryStatus.CANCELLED:
            raise RuntimeError(f"BigQuery job {job_id} was cancelled.")
        if status == QueryStatus.FAILED:
            raise RuntimeError(f"BigQuery job {job_id} failed.")
        if time.monotonic() - started_at >= query_timeout_seconds:
            await executor.cancel(job_id)
            raise TimeoutError(f"BigQuery job {job_id} exceeded {query_timeout_seconds}s timeout.")
        await asyncio.sleep(poll_interval_seconds)


async def _fetch_with_guardrails(
    executor: BigQueryAsyncQueryExecutor,
    sql: str,
    params: list,
    query_timeout_seconds: int,
    poll_interval_seconds: int,
    max_rows: int,
) -> List[Dict[str, Any]]:
    logger = logging.getLogger(__name__)
    job_id = await executor.submit(sql, params)
    started_at = time.monotonic()
    await _poll_until_done(
        executor,
        job_id,
        query_timeout_seconds=query_timeout_seconds,
        poll_interval_seconds=poll_interval_seconds,
    )
    rows = await executor.fetch(job_id, max_rows=max_rows)
    elapsed = time.monotonic() - started_at
    if elapsed >= query_timeout_seconds:
        logger.warning("BigQuery job %s took %.2fs.", job_id, elapsed)
    if max_rows and len(rows) >= max_rows:
        logger.warning("BigQuery job %s hit max rows cap (%s).", job_id, max_rows)
    return rows


def _columns_from_schema(schema: list) -> list:
    """Build column metadata from BigQuery schema fields."""
    from dal.util.column_metadata import build_column_meta
    from dal.util.logical_types import logical_type_from_db_type

    columns = []
    for field in schema or []:
        name = getattr(field, "name", None)
        db_type = getattr(field, "field_type", None) or getattr(field, "type", None)
        mode = getattr(field, "mode", None)
        nullable = mode == "NULLABLE" if mode is not None else None
        logical_type = logical_type_from_db_type(db_type, provider="bigquery")
        columns.append(build_column_meta(name, logical_type, db_type=db_type, nullable=nullable))
    return columns


async def _fetch_with_guardrails_with_columns(
    executor: BigQueryAsyncQueryExecutor,
    sql: str,
    params: list,
    query_timeout_seconds: int,
    poll_interval_seconds: int,
    max_rows: int,
) -> tuple[List[Dict[str, Any]], list]:
    """Fetch rows and columns with guardrails."""
    logger = logging.getLogger(__name__)
    job_id = await executor.submit(sql, params)
    started_at = time.monotonic()
    await _poll_until_done(
        executor,
        job_id,
        query_timeout_seconds=query_timeout_seconds,
        poll_interval_seconds=poll_interval_seconds,
    )
    rows, schema = await executor.fetch_with_schema(job_id, max_rows=max_rows)
    elapsed = time.monotonic() - started_at
    if elapsed >= query_timeout_seconds:
        logger.warning("BigQuery job %s took %.2fs.", job_id, elapsed)
    if max_rows and len(rows) >= max_rows:
        logger.warning("BigQuery job %s hit max rows cap (%s).", job_id, max_rows)
    return rows, _columns_from_schema(schema)
