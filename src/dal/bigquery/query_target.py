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


class BigQueryQueryTargetDatabase:
    """BigQuery query-target database wrapper."""

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
        """Yield a BigQuery connection wrapper (tenant context is a no-op)."""
        _ = tenant_id
        _ = read_only
        if cls._config is None:
            raise RuntimeError(
                "BigQuery config not initialized. Call BigQueryQueryTargetDatabase.init()."
            )
        executor = BigQueryAsyncQueryExecutor(
            project=cls._config.project,
            location=cls._config.location,
            timeout_seconds=cls._config.query_timeout_seconds,
            max_rows=cls._config.max_rows,
        )
        wrapper = _BigQueryConnection(
            executor=executor,
            query_timeout_seconds=cls._config.query_timeout_seconds,
            poll_interval_seconds=cls._config.poll_interval_seconds,
            max_rows=cls._config.max_rows,
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
    ) -> None:
        self._executor = executor
        self._query_timeout_seconds = query_timeout_seconds
        self._poll_interval_seconds = poll_interval_seconds
        self._max_rows = max_rows

    async def execute(self, sql: str, *params: Any) -> str:
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
        sql, query_params = translate_postgres_params_to_bigquery(sql, list(params))
        return await trace_query_operation(
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
