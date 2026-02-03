import asyncio
import types

import pytest

from dal.async_query_executor import QueryStatus
from dal.athena.executor import AthenaAsyncQueryExecutor


class _FakeAthenaClient:
    def __init__(self):
        self._status = "SUCCEEDED"
        self._rows = [
            {"Data": [{"VarCharValue": "col1"}]},
            {"Data": [{"VarCharValue": "value1"}]},
        ]

    def start_query_execution(
        self,
        QueryString,
        QueryExecutionContext,
        WorkGroup,
        ResultConfiguration,
        ExecutionParameters,
    ):
        _ = QueryString, QueryExecutionContext, WorkGroup, ResultConfiguration, ExecutionParameters
        return {"QueryExecutionId": "exec-1"}

    def get_query_execution(self, QueryExecutionId):
        _ = QueryExecutionId
        return {"QueryExecution": {"Status": {"State": self._status}}}

    def get_query_results(self, QueryExecutionId, MaxResults, NextToken=None):
        _ = QueryExecutionId, MaxResults, NextToken
        return {
            "ResultSet": {
                "ResultSetMetadata": {"ColumnInfo": [{"Name": "col1"}]},
                "Rows": list(self._rows),
            }
        }

    def stop_query_execution(self, QueryExecutionId):
        _ = QueryExecutionId
        self._status = "CANCELLED"


@pytest.mark.asyncio
async def test_athena_executor_submit_poll_fetch(monkeypatch):
    """Validate submit/poll/fetch wiring for Athena executor."""
    fake_boto3 = types.SimpleNamespace(client=lambda service, region_name=None: _FakeAthenaClient())
    monkeypatch.setitem(__import__("sys").modules, "boto3", fake_boto3)

    executor = AthenaAsyncQueryExecutor(
        region="us-east-1",
        workgroup="primary",
        output_location="s3://bucket/out/",
        database="db",
        timeout_seconds=5,
        max_rows=1000,
    )
    job_id = await executor.submit("SELECT 1", params=[])
    assert job_id == "exec-1"

    status = await executor.poll(job_id)
    assert status == QueryStatus.SUCCEEDED

    rows = await executor.fetch(job_id, max_rows=10)
    assert rows == [{"col1": "value1"}]


class _FakePaginatedAthenaClient:
    """Simulates Athena client that returns results with NextToken pagination."""

    def __init__(self):
        self._status = "SUCCEEDED"
        self._call_count = 0
        self._max_results_calls = []

    def start_query_execution(self, **kwargs):
        return {"QueryExecutionId": "paginated-exec-1"}

    def get_query_execution(self, QueryExecutionId):
        return {"QueryExecution": {"Status": {"State": self._status}}}

    def get_query_results(self, QueryExecutionId, MaxResults, NextToken=None):
        """Simulate paginated results: first call returns NextToken, second does not."""
        self._call_count += 1
        self._max_results_calls.append(MaxResults)

        if NextToken is None:
            # First page: header row + 2 data rows + NextToken
            return {
                "ResultSet": {
                    "ResultSetMetadata": {"ColumnInfo": [{"Name": "id"}, {"Name": "name"}]},
                    "Rows": [
                        {"Data": [{"VarCharValue": "id"}, {"VarCharValue": "name"}]},  # Header
                        {"Data": [{"VarCharValue": "1"}, {"VarCharValue": "Alice"}]},
                        {"Data": [{"VarCharValue": "2"}, {"VarCharValue": "Bob"}]},
                    ],
                },
                "NextToken": "page2-token",
            }
        else:
            # Second page: no header, 2 more data rows, no NextToken
            return {
                "ResultSet": {
                    "ResultSetMetadata": {"ColumnInfo": [{"Name": "id"}, {"Name": "name"}]},
                    "Rows": [
                        {"Data": [{"VarCharValue": "3"}, {"VarCharValue": "Charlie"}]},
                        {"Data": [{"VarCharValue": "4"}, {"VarCharValue": "Diana"}]},
                    ],
                },
            }

    def stop_query_execution(self, QueryExecutionId):
        self._status = "CANCELLED"


@pytest.mark.asyncio
async def test_athena_executor_pagination_combines_pages(monkeypatch):
    """Verify fetch correctly handles NextToken pagination and combines all pages."""
    fake_client = _FakePaginatedAthenaClient()
    fake_boto3 = types.SimpleNamespace(client=lambda service, region_name=None: fake_client)
    monkeypatch.setitem(__import__("sys").modules, "boto3", fake_boto3)

    executor = AthenaAsyncQueryExecutor(
        region="us-east-1",
        workgroup="primary",
        output_location="s3://bucket/out/",
        database="db",
        timeout_seconds=5,
        max_rows=1000,  # High limit to fetch all pages
    )

    rows = await executor.fetch("paginated-exec-1", max_rows=1000)

    # Should have called get_query_results twice (for both pages)
    assert fake_client._call_count == 2

    # Should have combined 4 data rows from both pages (header row skipped)
    assert len(rows) == 4
    assert rows[0] == {"id": "1", "name": "Alice"}
    assert rows[1] == {"id": "2", "name": "Bob"}
    assert rows[2] == {"id": "3", "name": "Charlie"}
    assert rows[3] == {"id": "4", "name": "Diana"}


@pytest.mark.asyncio
async def test_athena_executor_pagination_respects_max_rows(monkeypatch):
    """Verify fetch stops pagination when max_rows is reached."""
    fake_client = _FakePaginatedAthenaClient()
    fake_boto3 = types.SimpleNamespace(client=lambda service, region_name=None: fake_client)
    monkeypatch.setitem(__import__("sys").modules, "boto3", fake_boto3)

    executor = AthenaAsyncQueryExecutor(
        region="us-east-1",
        workgroup="primary",
        output_location="s3://bucket/out/",
        database="db",
        timeout_seconds=5,
        max_rows=2,  # Only allow 2 rows
    )

    rows = await executor.fetch("paginated-exec-1", max_rows=2)

    # Should stop after reaching max_rows (may only fetch first page)
    assert len(rows) == 2
    assert rows[0] == {"id": "1", "name": "Alice"}
    assert rows[1] == {"id": "2", "name": "Bob"}
    assert fake_client._call_count == 1
    assert fake_client._max_results_calls == [3]


@pytest.mark.asyncio
async def test_athena_executor_pagination_fetches_remaining(monkeypatch):
    """Verify pagination fetches only remaining rows on later pages."""
    fake_client = _FakePaginatedAthenaClient()
    fake_boto3 = types.SimpleNamespace(client=lambda service, region_name=None: fake_client)
    monkeypatch.setitem(__import__("sys").modules, "boto3", fake_boto3)

    executor = AthenaAsyncQueryExecutor(
        region="us-east-1",
        workgroup="primary",
        output_location="s3://bucket/out/",
        database="db",
        timeout_seconds=5,
        max_rows=3,
    )

    rows = await executor.fetch("paginated-exec-1", max_rows=3)

    assert len(rows) == 3
    assert rows[0] == {"id": "1", "name": "Alice"}
    assert rows[2] == {"id": "3", "name": "Charlie"}
    assert fake_client._call_count == 2
    assert fake_client._max_results_calls == [4, 1]


@pytest.mark.asyncio
async def test_athena_executor_skips_header_row(monkeypatch):
    """Verify header row (matching column names) is correctly skipped."""
    fake_client = _FakeAthenaClient()
    # Override with header row that matches column names
    fake_client._rows = [
        {"Data": [{"VarCharValue": "col1"}]},  # Header row
        {"Data": [{"VarCharValue": "actual_value"}]},  # Data row
    ]

    fake_boto3 = types.SimpleNamespace(client=lambda service, region_name=None: fake_client)
    monkeypatch.setitem(__import__("sys").modules, "boto3", fake_boto3)

    executor = AthenaAsyncQueryExecutor(
        region="us-east-1",
        workgroup="primary",
        output_location="s3://bucket/out/",
        database="db",
        timeout_seconds=5,
        max_rows=1000,
    )

    rows = await executor.fetch("exec-1", max_rows=100)

    # Should only have the data row, header skipped
    assert len(rows) == 1
    assert rows[0] == {"col1": "actual_value"}


@pytest.mark.asyncio
async def test_athena_executor_header_value_not_dropped(monkeypatch):
    """Verify a data row equal to header values is not dropped."""
    fake_client = _FakeAthenaClient()
    fake_client._rows = [
        {"Data": [{"VarCharValue": "col1"}]},  # Header row
        {"Data": [{"VarCharValue": "col1"}]},  # Data row with same value
    ]

    fake_boto3 = types.SimpleNamespace(client=lambda service, region_name=None: fake_client)
    monkeypatch.setitem(__import__("sys").modules, "boto3", fake_boto3)

    executor = AthenaAsyncQueryExecutor(
        region="us-east-1",
        workgroup="primary",
        output_location="s3://bucket/out/",
        database="db",
        timeout_seconds=5,
        max_rows=1000,
    )

    rows = await executor.fetch("exec-1", max_rows=100)

    assert len(rows) == 1
    assert rows[0] == {"col1": "col1"}


@pytest.mark.asyncio
async def test_athena_executor_cancel(monkeypatch):
    """Verify cancel calls stop_query_execution."""
    fake_client = _FakeAthenaClient()
    fake_boto3 = types.SimpleNamespace(client=lambda service, region_name=None: fake_client)
    monkeypatch.setitem(__import__("sys").modules, "boto3", fake_boto3)

    executor = AthenaAsyncQueryExecutor(
        region="us-east-1",
        workgroup="primary",
        output_location="s3://bucket/out/",
        database="db",
        timeout_seconds=5,
        max_rows=1000,
    )

    await executor.cancel("exec-1")

    # Client status should be updated to CANCELLED
    assert fake_client._status == "CANCELLED"


@pytest.mark.asyncio
async def test_athena_status_mapping(monkeypatch):
    """Verify all Athena status values map correctly."""
    from dal.athena.executor import _map_status

    assert _map_status("SUCCEEDED") == QueryStatus.SUCCEEDED
    assert _map_status("FAILED") == QueryStatus.FAILED
    assert _map_status("CANCELLED") == QueryStatus.CANCELLED
    assert _map_status("QUEUED") == QueryStatus.RUNNING
    assert _map_status("RUNNING") == QueryStatus.RUNNING


@pytest.mark.asyncio
async def test_athena_executor_fetch_timeout_calls_cancel(monkeypatch):
    """Verify fetch timeout triggers stop_query_execution."""

    class _SlowAthenaClient(_FakeAthenaClient):
        def __init__(self):
            super().__init__()
            self._stopped_ids = []

        def get_query_results(self, QueryExecutionId, MaxResults, NextToken=None):
            _ = QueryExecutionId, MaxResults, NextToken
            import time

            time.sleep(0.05)
            return super().get_query_results(QueryExecutionId, MaxResults, NextToken)

        def stop_query_execution(self, QueryExecutionId):
            self._stopped_ids.append(QueryExecutionId)
            super().stop_query_execution(QueryExecutionId)

    fake_client = _SlowAthenaClient()
    fake_boto3 = types.SimpleNamespace(client=lambda service, region_name=None: fake_client)
    monkeypatch.setitem(__import__("sys").modules, "boto3", fake_boto3)

    executor = AthenaAsyncQueryExecutor(
        region="us-east-1",
        workgroup="primary",
        output_location="s3://bucket/out/",
        database="db",
        timeout_seconds=0.001,
        max_rows=1000,
    )

    with pytest.raises(asyncio.TimeoutError):
        await executor.fetch("exec-1", max_rows=10)

    assert fake_client._status == "CANCELLED"
    assert fake_client._stopped_ids == ["exec-1"]
