import asyncio
from typing import Any, Dict, List, Optional

from dal.async_query_executor import AsyncQueryExecutor
from dal.async_query_executor import QueryStatus as NormalizedStatus
from dal.async_utils import with_timeout
from dal.tracing import trace_query_operation


class AthenaAsyncQueryExecutor(AsyncQueryExecutor):
    """AsyncQueryExecutor backed by Athena query executions."""

    def __init__(
        self,
        region: str,
        workgroup: str,
        output_location: str,
        database: str,
        timeout_seconds: int,
        max_rows: int,
    ) -> None:
        """Initialize executor with Athena connection settings."""
        import boto3

        self._client = boto3.client("athena", region_name=region)
        self._workgroup = workgroup
        self._output_location = output_location
        self._database = database
        self._timeout_seconds = timeout_seconds
        self._max_rows = max_rows

    async def submit(self, sql: str, params: Optional[list] = None) -> str:
        """Submit a query for asynchronous execution."""
        return await trace_query_operation(
            "dal.query.submit",
            provider="athena",
            execution_model="async",
            sql=sql,
            operation=asyncio.to_thread(
                _start_query_execution,
                self._client,
                sql,
                self._database,
                self._workgroup,
                self._output_location,
                params or [],
            ),
        )

    async def poll(self, job_id: str) -> NormalizedStatus:
        """Poll the status of a running query."""
        status = await trace_query_operation(
            "dal.query.poll",
            provider="athena",
            execution_model="async",
            sql=None,
            operation=asyncio.to_thread(_get_query_status, self._client, job_id),
        )
        return _map_status(status)

    async def fetch(self, job_id: str, max_rows: Optional[int] = None) -> List[Dict[str, Any]]:
        """Fetch results for a completed query."""
        limit = max_rows if max_rows is not None else self._max_rows
        operation = with_timeout(
            asyncio.to_thread(_fetch_results, self._client, job_id, limit),
            timeout_seconds=self._timeout_seconds,
            on_timeout=lambda: self.cancel(job_id),
        )
        return await trace_query_operation(
            "dal.query.fetch",
            provider="athena",
            execution_model="async",
            sql=None,
            operation=operation,
        )

    async def fetch_with_columns(
        self, job_id: str, max_rows: Optional[int] = None
    ) -> tuple[List[Dict[str, Any]], list]:
        """Fetch results and column metadata for a completed query."""
        limit = max_rows if max_rows is not None else self._max_rows
        operation = with_timeout(
            asyncio.to_thread(_fetch_results_with_columns, self._client, job_id, limit),
            timeout_seconds=self._timeout_seconds,
            on_timeout=lambda: self.cancel(job_id),
        )
        return await trace_query_operation(
            "dal.query.fetch",
            provider="athena",
            execution_model="async",
            sql=None,
            operation=operation,
        )

    async def cancel(self, job_id: str) -> None:
        """Cancel a running query."""
        await asyncio.to_thread(self._client.stop_query_execution, QueryExecutionId=job_id)


def _start_query_execution(
    client,
    sql: str,
    database: str,
    workgroup: str,
    output_location: str,
    params: list,
) -> str:
    response = client.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": database},
        WorkGroup=workgroup,
        ResultConfiguration={"OutputLocation": output_location},
        ExecutionParameters=params,
    )
    return response["QueryExecutionId"]


def _get_query_status(client, job_id: str) -> str:
    response = client.get_query_execution(QueryExecutionId=job_id)
    return response["QueryExecution"]["Status"]["State"]


def _fetch_results(client, job_id: str, max_rows: int) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    next_token = None
    column_names: Optional[List[str]] = None
    header_skipped = False

    while True:
        remaining = max_rows - len(rows)
        if remaining <= 0:
            break
        max_results = remaining + (0 if header_skipped else 1)
        kwargs = {"QueryExecutionId": job_id, "MaxResults": max_results}
        if next_token:
            kwargs["NextToken"] = next_token
        response = client.get_query_results(**kwargs)
        result_set = response["ResultSet"]
        metadata = result_set["ResultSetMetadata"]["ColumnInfo"]
        if column_names is None:
            column_names = [col["Name"] for col in metadata]
        page_rows = result_set["Rows"]
        start_index = 0
        if not header_skipped and page_rows:
            start_index = 1
            header_skipped = True
        for row in page_rows[start_index:]:
            values = [datum.get("VarCharValue") for datum in row["Data"]]
            rows.append(dict(zip(column_names, values)))
            if len(rows) >= max_rows:
                return rows
        next_token = response.get("NextToken")
        if not next_token:
            break
    return rows


def _fetch_results_with_columns(
    client, job_id: str, max_rows: int
) -> tuple[List[Dict[str, Any]], list]:
    """Fetch rows and column metadata for Athena query results."""
    rows: List[Dict[str, Any]] = []
    next_token = None
    column_names: Optional[List[str]] = None
    column_meta: list = []
    header_skipped = False

    while True:
        remaining = max_rows - len(rows)
        if remaining <= 0:
            break
        max_results = remaining + (0 if header_skipped else 1)
        kwargs = {"QueryExecutionId": job_id, "MaxResults": max_results}
        if next_token:
            kwargs["NextToken"] = next_token
        response = client.get_query_results(**kwargs)
        result_set = response["ResultSet"]
        metadata = result_set["ResultSetMetadata"]["ColumnInfo"]
        if column_names is None:
            column_names = [col["Name"] for col in metadata]
            column_meta = _columns_from_athena_metadata(metadata)
        page_rows = result_set["Rows"]
        start_index = 0
        if not header_skipped and page_rows:
            start_index = 1
            header_skipped = True
        for row in page_rows[start_index:]:
            values = [datum.get("VarCharValue") for datum in row["Data"]]
            rows.append(dict(zip(column_names, values)))
            if len(rows) >= max_rows:
                return rows, column_meta
        next_token = response.get("NextToken")
        if not next_token:
            break
    return rows, column_meta


def _columns_from_athena_metadata(metadata: list) -> list:
    """Build column metadata from Athena ColumnInfo payloads."""
    from dal.util.column_metadata import build_column_meta
    from dal.util.logical_types import logical_type_from_db_type

    columns = []
    for col in metadata or []:
        db_type = col.get("Type")
        logical_type = logical_type_from_db_type(db_type, provider="athena")
        columns.append(
            build_column_meta(
                col.get("Name"),
                logical_type,
                db_type=db_type,
                nullable=col.get("Nullable"),
            )
        )
    return columns


def _map_status(status: str) -> NormalizedStatus:
    if status == "SUCCEEDED":
        return NormalizedStatus.SUCCEEDED
    if status == "FAILED":
        return NormalizedStatus.FAILED
    if status == "CANCELLED":
        return NormalizedStatus.CANCELLED
    return NormalizedStatus.RUNNING
