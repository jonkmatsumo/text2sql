import asyncio
import json
import urllib.request
from typing import Any, Dict, List, Optional

from dal.async_query_executor import AsyncQueryExecutor
from dal.async_query_executor import QueryStatus as NormalizedStatus
from dal.async_utils import with_timeout
from dal.tracing import trace_query_operation


class DatabricksAsyncQueryExecutor(AsyncQueryExecutor):
    """AsyncQueryExecutor backed by Databricks Statement Execution API."""

    def __init__(
        self,
        host: str,
        token: str,
        warehouse_id: str,
        catalog: str,
        schema: str,
        timeout_seconds: int,
        max_rows: int,
        read_only: bool = False,
    ) -> None:
        """Initialize executor with Databricks SQL Warehouse settings."""
        self._host = host.rstrip("/")
        self._token = token
        self._warehouse_id = warehouse_id
        self._catalog = catalog
        self._schema = schema
        self._timeout_seconds = timeout_seconds
        self._max_rows = max_rows
        self._read_only = read_only

    async def submit(self, sql: str, params: Optional[list] = None) -> str:
        """Submit a query for asynchronous execution."""
        from dal.util.read_only import enforce_read_only_sql, validate_no_mutation_keywords

        enforce_read_only_sql(sql, provider="databricks", read_only=self._read_only)
        if self._read_only:
            validate_no_mutation_keywords(sql)
        payload = {
            "statement": sql,
            "warehouse_id": self._warehouse_id,
            "catalog": self._catalog,
            "schema": self._schema,
        }
        if params:
            payload["parameters"] = params
        operation = with_timeout(
            asyncio.to_thread(
                _request,
                "POST",
                f"{self._host}/api/2.0/sql/statements",
                self._token,
                payload,
                self._timeout_seconds,
            ),
            timeout_seconds=self._timeout_seconds,
        )
        response = await trace_query_operation(
            "dal.query.submit",
            provider="databricks",
            execution_model="async",
            sql=sql,
            operation=operation,
        )
        return response["statement_id"]

    async def poll(self, job_id: str) -> NormalizedStatus:
        """Poll the status of a running query."""
        operation = with_timeout(
            asyncio.to_thread(
                _request,
                "GET",
                f"{self._host}/api/2.0/sql/statements/{job_id}",
                self._token,
                None,
                self._timeout_seconds,
            ),
            timeout_seconds=self._timeout_seconds,
        )
        response = await trace_query_operation(
            "dal.query.poll",
            provider="databricks",
            execution_model="async",
            sql=None,
            operation=operation,
        )
        state = response.get("status", {}).get("state") or response.get("state")
        return _map_status(state)

    async def fetch(self, job_id: str, max_rows: Optional[int] = None) -> List[Dict[str, Any]]:
        """Fetch results for a completed query."""
        limit = max_rows if max_rows is not None else self._max_rows
        operation = with_timeout(
            asyncio.to_thread(
                _fetch_results,
                self._host,
                self._token,
                job_id,
                limit,
                self._timeout_seconds,
            ),
            timeout_seconds=self._timeout_seconds,
            on_timeout=lambda: self.cancel(job_id),
        )
        return await trace_query_operation(
            "dal.query.fetch",
            provider="databricks",
            execution_model="async",
            sql=None,
            operation=operation,
        )

    async def fetch_with_columns(
        self, job_id: str, max_rows: Optional[int] = None
    ) -> tuple[List[Dict[str, Any]], list]:
        """Fetch results with column metadata for a completed query."""
        limit = max_rows if max_rows is not None else self._max_rows
        operation = with_timeout(
            asyncio.to_thread(
                _fetch_results_with_columns,
                self._host,
                self._token,
                job_id,
                limit,
                self._timeout_seconds,
            ),
            timeout_seconds=self._timeout_seconds,
            on_timeout=lambda: self.cancel(job_id),
        )
        return await trace_query_operation(
            "dal.query.fetch",
            provider="databricks",
            execution_model="async",
            sql=None,
            operation=operation,
        )

    async def cancel(self, job_id: str) -> None:
        """Cancel a running query."""
        await asyncio.to_thread(
            _request,
            "POST",
            f"{self._host}/api/2.0/sql/statements/{job_id}/cancel",
            self._token,
            None,
            self._timeout_seconds,
        )


def _request(method: str, url: str, token: str, payload: Optional[dict], timeout: float) -> dict:
    data = None
    headers = {"Authorization": f"Bearer {token}"}
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"
    request = urllib.request.Request(url, data=data, headers=headers, method=method)
    with urllib.request.urlopen(request, timeout=timeout) as response:
        body = response.read().decode("utf-8")
        return json.loads(body) if body else {}


def _fetch_results(
    host: str, token: str, job_id: str, max_rows: int, timeout: float
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    response = _request(
        "GET",
        f"{host}/api/2.0/sql/statements/{job_id}",
        token,
        None,
        timeout,
    )
    rows.extend(_parse_result(response))

    next_link = _get_next_chunk_link(response)
    while next_link and len(rows) < max_rows:
        url = next_link if next_link.startswith("http") else f"{host}{next_link}"
        chunk = _request("GET", url, token, None, timeout)
        rows.extend(_parse_result(chunk))
        next_link = _get_next_chunk_link(chunk)

    return rows[:max_rows]


def _fetch_results_with_columns(
    host: str, token: str, job_id: str, max_rows: int, timeout: float
) -> tuple[List[Dict[str, Any]], list]:
    """Fetch results and column metadata for a statement."""
    rows: List[Dict[str, Any]] = []
    response = _request(
        "GET",
        f"{host}/api/2.0/sql/statements/{job_id}",
        token,
        None,
        timeout,
    )
    chunk_rows, columns = _parse_result_with_columns(response)
    rows.extend(chunk_rows)

    next_link = _get_next_chunk_link(response)
    while next_link and len(rows) < max_rows:
        url = next_link if next_link.startswith("http") else f"{host}{next_link}"
        chunk = _request("GET", url, token, None, timeout)
        rows.extend(_parse_result(chunk))
        next_link = _get_next_chunk_link(chunk)

    return rows[:max_rows], columns


def _parse_result(response: dict) -> List[Dict[str, Any]]:
    result = response.get("result") or {}
    data_array = result.get("data_array") or []
    schema = result.get("schema") or {}
    columns = schema.get("columns") or []
    column_names = [col.get("name") for col in columns]
    return [dict(zip(column_names, row)) for row in data_array]


def _parse_result_with_columns(response: dict) -> tuple[List[Dict[str, Any]], list]:
    """Parse result payload into rows and columns."""
    result = response.get("result") or {}
    data_array = result.get("data_array") or []
    schema = result.get("schema") or {}
    columns = _columns_from_databricks_schema(schema.get("columns") or [])
    column_names = [col.get("name") for col in schema.get("columns") or []]
    return [dict(zip(column_names, row)) for row in data_array], columns


def _columns_from_databricks_schema(columns: list) -> list:
    """Build column metadata from Databricks schema payload."""
    from dal.util.column_metadata import build_column_meta
    from dal.util.logical_types import logical_type_from_db_type

    metadata = []
    for col in columns or []:
        name = col.get("name")
        db_type = col.get("type_name") or col.get("type") or col.get("type_text")
        logical_type = logical_type_from_db_type(db_type, provider="databricks")
        metadata.append(
            build_column_meta(
                name,
                logical_type,
                db_type=db_type,
                nullable=col.get("nullable"),
            )
        )
    return metadata


def _get_next_chunk_link(response: dict) -> Optional[str]:
    result = response.get("result") or {}
    return result.get("next_chunk_internal_link")


def _map_status(state: Optional[str]) -> NormalizedStatus:
    if state == "SUCCEEDED":
        return NormalizedStatus.SUCCEEDED
    if state in {"FAILED", "CANCELED"}:
        return NormalizedStatus.FAILED if state == "FAILED" else NormalizedStatus.CANCELLED
    return NormalizedStatus.RUNNING
