import asyncio
import types

import pytest

from dal.async_query_executor import QueryStatus
from dal.bigquery.executor import BigQueryAsyncQueryExecutor


class _FakeJob:
    def __init__(self, job_id):
        self.job_id = job_id
        self.state = "DONE"
        self.error_result = None
        self._cancelled = False

    def cancelled(self):
        return self._cancelled

    def result(self, max_results=None, timeout=None):
        _ = max_results, timeout
        return [{"ok": 1}]


class _FakeClient:
    def __init__(self, project=None):
        self.project = project
        self._job = _FakeJob("job-123")

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


@pytest.mark.asyncio
async def test_bigquery_executor_submit_poll_fetch(monkeypatch):
    """Validate submit/poll/fetch wiring for BigQuery executor."""
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
        timeout_seconds=5,
        max_rows=1000,
    )
    job_id = await executor.submit("SELECT 1", params=[])
    assert job_id == "job-123"

    status = await executor.poll(job_id)
    assert status == QueryStatus.SUCCEEDED

    rows = await executor.fetch(job_id, max_rows=10)
    assert rows == [{"ok": 1}]


@pytest.mark.asyncio
async def test_bigquery_executor_fetch_timeout_calls_cancel(monkeypatch):
    """Verify fetch timeout triggers job cancel."""

    class _SlowJob(_FakeJob):
        def result(self, max_results=None, timeout=None):
            _ = max_results, timeout
            import time

            time.sleep(0.05)
            return [{"ok": 1}]

    class _SlowClient(_FakeClient):
        def __init__(self, project=None):
            super().__init__(project=project)
            self._job = _SlowJob("job-123")

    fake_bigquery = types.SimpleNamespace(Client=_SlowClient, QueryJobConfig=_FakeQueryJobConfig)
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
