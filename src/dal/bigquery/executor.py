import asyncio
from typing import Any, Dict, List, Optional

from dal.async_query_executor import AsyncQueryExecutor
from dal.async_query_executor import QueryStatus as NormalizedStatus
from dal.async_utils import with_timeout
from dal.tracing import trace_query_operation


class BigQueryAsyncQueryExecutor(AsyncQueryExecutor):
    """AsyncQueryExecutor backed by BigQuery QueryJobs."""

    def __init__(
        self,
        project: str,
        location: Optional[str],
        timeout_seconds: int,
        max_rows: int,
        read_only: bool = False,
    ) -> None:
        """Initialize executor with project and execution limits."""
        from google.cloud import bigquery

        self._client = bigquery.Client(project=project)
        self._location = location
        self._timeout_seconds = timeout_seconds
        self._max_rows = max_rows
        self._read_only = read_only

    async def submit(self, sql: str, params: Optional[list] = None) -> str:
        """Submit a query for asynchronous execution."""
        from google.cloud import bigquery

        from dal.util.read_only import enforce_read_only_sql, validate_no_mutation_keywords

        enforce_read_only_sql(sql, provider="bigquery", read_only=self._read_only)
        if self._read_only:
            validate_no_mutation_keywords(sql)
        job_config = bigquery.QueryJobConfig()
        if params:
            job_config.query_parameters = params
        return await trace_query_operation(
            "dal.query.submit",
            provider="bigquery",
            execution_model="async",
            sql=sql,
            operation=asyncio.to_thread(
                _submit,
                self._client,
                sql,
                job_config,
                self._location,
            ),
        )

    async def poll(self, job_id: str) -> NormalizedStatus:
        """Poll the status of a running query."""
        job = await trace_query_operation(
            "dal.query.poll",
            provider="bigquery",
            execution_model="async",
            sql=None,
            operation=asyncio.to_thread(_get_job, self._client, job_id, self._location),
        )
        if job.cancelled():
            return NormalizedStatus.CANCELLED
        if job.state == "DONE":
            return NormalizedStatus.FAILED if job.error_result else NormalizedStatus.SUCCEEDED
        return NormalizedStatus.RUNNING

    async def fetch(self, job_id: str, max_rows: Optional[int] = None) -> List[Dict[str, Any]]:
        """Fetch results for a completed query."""
        job = await trace_query_operation(
            "dal.query.fetch",
            provider="bigquery",
            execution_model="async",
            sql=None,
            operation=asyncio.to_thread(_get_job, self._client, job_id, self._location),
        )
        limit = max_rows if max_rows is not None else self._max_rows
        return await with_timeout(
            asyncio.to_thread(
                _fetch_rows,
                job,
                limit,
                self._timeout_seconds,
            ),
            timeout_seconds=self._timeout_seconds,
            on_timeout=lambda: self.cancel(job_id),
        )

    async def fetch_with_schema(
        self, job_id: str, max_rows: Optional[int] = None
    ) -> tuple[List[Dict[str, Any]], list]:
        """Fetch results and schema for a completed query."""
        job = await trace_query_operation(
            "dal.query.fetch",
            provider="bigquery",
            execution_model="async",
            sql=None,
            operation=asyncio.to_thread(_get_job, self._client, job_id, self._location),
        )
        limit = max_rows if max_rows is not None else self._max_rows
        rows = await with_timeout(
            asyncio.to_thread(
                _fetch_rows,
                job,
                limit,
                self._timeout_seconds,
            ),
            timeout_seconds=self._timeout_seconds,
            on_timeout=lambda: self.cancel(job_id),
        )
        schema = job.schema or []
        return rows, schema

    async def cancel(self, job_id: str) -> None:
        """Cancel a running query."""
        await asyncio.to_thread(self._client.cancel_job, job_id, location=self._location)


def _submit(
    client,
    sql: str,
    job_config,
    location: Optional[str],
) -> str:
    job = client.query(sql, job_config=job_config, location=location)
    return job.job_id


def _get_job(client, job_id: str, location: Optional[str]):
    return client.get_job(job_id, location=location)


def _fetch_rows(job, max_rows: int, timeout_seconds: int) -> List[Dict[str, Any]]:
    iterator = job.result(max_results=max_rows, timeout=timeout_seconds)
    return [dict(row) for row in iterator]
