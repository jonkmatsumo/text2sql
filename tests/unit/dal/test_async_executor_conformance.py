"""Async executor protocol conformance tests (mock-only)."""

import asyncio
import types

import pytest

from dal.async_query_executor import QueryStatus


@pytest.mark.asyncio
async def test_snowflake_executor_conformance():
    """Validate Snowflake executor submit/poll/fetch behavior."""
    from snowflake.connector.constants import QueryStatus as SfStatus

    from dal.snowflake.executor import SnowflakeAsyncQueryExecutor

    class _SubmitCursor:
        def __init__(self):
            self.sfqid = "sfqid-1"

        def execute_async(self, sql, params):
            _ = sql, params

    class _CursorContext:
        def __init__(self, cursor):
            self._cursor = cursor

        def __enter__(self):
            return self._cursor

        def __exit__(self, exc_type, exc, tb):
            return None

    class _FetchCursor:
        def __init__(self):
            self.description = [("id", None)]
            self.fetchmany_calls = 0

        def fetchmany(self, size):
            _ = size
            self.fetchmany_calls += 1
            if self.fetchmany_calls == 1:
                return [(1,), (2,)]
            if self.fetchmany_calls == 2:
                return [(3,)]
            return []

    class _FakeConn:
        def __init__(self):
            self._submit_cursor = _SubmitCursor()
            self._fetch_cursor = _FetchCursor()
            self.cancelled = False

        def cursor(self):
            return _CursorContext(self._submit_cursor)

        def get_query_status(self, job_id):
            _ = job_id
            return SfStatus.SUCCESS

        def get_results_from_sfqid(self, job_id):
            _ = job_id
            return self._fetch_cursor

        def cancel_query(self, job_id):
            _ = job_id
            self.cancelled = True

    conn = _FakeConn()
    executor = SnowflakeAsyncQueryExecutor(conn)

    job_id = await executor.submit("SELECT 1", params=None)
    assert job_id == "sfqid-1"

    status = await executor.poll(job_id)
    assert status == QueryStatus.SUCCEEDED

    rows = await executor.fetch(job_id, max_rows=2)
    assert rows == [{"id": 1}, {"id": 2}]
    assert conn._fetch_cursor.fetchmany_calls >= 1


@pytest.mark.asyncio
async def test_bigquery_executor_conformance_timeout_cancel(monkeypatch):
    """Ensure BigQuery executor cancels job on timeout."""
    from dal.bigquery.executor import BigQueryAsyncQueryExecutor

    class _SlowJob:
        def __init__(self):
            self.job_id = "job-123"
            self.state = "DONE"
            self.error_result = None
            self._cancelled = False

        def cancelled(self):
            return self._cancelled

        def result(self, max_results=None, timeout=None):
            _ = max_results, timeout
            import time

            time.sleep(0.05)
            return [{"ok": 1}]

    class _FakeClient:
        def __init__(self, project=None):
            _ = project
            self._job = _SlowJob()

        def query(self, sql, job_config=None, location=None):
            _ = sql, job_config, location
            return self._job

        def get_job(self, job_id, location=None):
            _ = job_id, location
            return self._job

        def cancel_job(self, job_id, location=None):
            _ = job_id, location
            self._job._cancelled = True

    class _FakeQueryJobConfig:
        def __init__(self):
            self.query_parameters = []

    fake_bigquery = types.SimpleNamespace(Client=_FakeClient, QueryJobConfig=_FakeQueryJobConfig)
    fake_cloud = types.SimpleNamespace(bigquery=fake_bigquery)
    monkeypatch.setitem(
        __import__("sys").modules, "google", types.SimpleNamespace(cloud=fake_cloud)
    )
    monkeypatch.setitem(__import__("sys").modules, "google.cloud", fake_cloud)
    monkeypatch.setitem(__import__("sys").modules, "google.cloud.bigquery", fake_bigquery)

    executor = BigQueryAsyncQueryExecutor(
        project="proj",
        location=None,
        timeout_seconds=0.001,
        max_rows=1000,
    )

    with pytest.raises(asyncio.TimeoutError):
        await executor.fetch("job-123", max_rows=10)

    assert executor._client._job._cancelled is True


@pytest.mark.asyncio
async def test_athena_executor_conformance_timeout_cancel(monkeypatch):
    """Ensure Athena executor cancels query on timeout."""
    import dal.athena.executor as executor_mod
    from dal.athena.executor import AthenaAsyncQueryExecutor

    class _FakeClient:
        def __init__(self):
            self.cancelled = False

        def stop_query_execution(self, QueryExecutionId):
            _ = QueryExecutionId
            self.cancelled = True

    fake_client = _FakeClient()
    fake_boto3 = types.SimpleNamespace(client=lambda service, region_name=None: fake_client)
    monkeypatch.setitem(__import__("sys").modules, "boto3", fake_boto3)

    def slow_fetch_results(client, job_id, max_rows):
        _ = client, job_id, max_rows
        import time

        time.sleep(0.05)
        return []

    monkeypatch.setattr(executor_mod, "_fetch_results", slow_fetch_results)

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

    assert fake_client.cancelled is True


@pytest.mark.asyncio
async def test_databricks_executor_conformance_timeout_cancel(monkeypatch):
    """Ensure Databricks executor cancels statement on timeout."""
    import dal.databricks.executor as executor_mod
    from dal.databricks.executor import DatabricksAsyncQueryExecutor

    calls = []

    def slow_request(method, url, token, payload, timeout):
        _ = token, payload, timeout
        calls.append((method, url))
        if method == "POST" and url.endswith("/cancel"):
            return {}
        if method == "GET":
            import time

            time.sleep(0.05)
            return {"status": {"state": "SUCCEEDED"}, "result": {"data_array": [], "schema": {}}}
        return {"statement_id": "stmt-1"}

    monkeypatch.setattr(executor_mod, "_request", slow_request)

    executor = DatabricksAsyncQueryExecutor(
        host="https://example.cloud.databricks.com",
        token="token",
        warehouse_id="wh",
        catalog="main",
        schema="public",
        timeout_seconds=0.001,
        max_rows=1000,
    )

    with pytest.raises(asyncio.TimeoutError):
        await executor.fetch("stmt-1", max_rows=10)

    assert any(url.endswith("/cancel") for _, url in calls)
