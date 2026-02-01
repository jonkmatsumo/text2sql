import pytest

import dal.databricks.executor as executor_mod
from dal.async_query_executor import QueryStatus
from dal.databricks.executor import DatabricksAsyncQueryExecutor


@pytest.mark.asyncio
async def test_databricks_executor_submit_poll_fetch(monkeypatch):
    """Validate submit/poll/fetch wiring for Databricks executor."""

    def fake_request(method, url, token, payload):
        _ = token, payload
        if method == "POST" and url.endswith("/api/2.0/sql/statements"):
            return {"statement_id": "stmt-1"}
        if method == "GET" and url.endswith("/api/2.0/sql/statements/stmt-1"):
            return {
                "status": {"state": "SUCCEEDED"},
                "result": {
                    "data_array": [[1]],
                    "schema": {"columns": [{"name": "id"}]},
                },
            }
        if method == "POST" and url.endswith("/cancel"):
            return {}
        raise AssertionError(f"Unexpected request: {method} {url}")

    monkeypatch.setattr(executor_mod, "_request", fake_request)

    executor = DatabricksAsyncQueryExecutor(
        host="https://example.cloud.databricks.com",
        token="token",
        warehouse_id="wh",
        catalog="main",
        schema="public",
        timeout_seconds=5,
        max_rows=1000,
    )
    job_id = await executor.submit("SELECT 1", params=[])
    assert job_id == "stmt-1"

    status = await executor.poll(job_id)
    assert status == QueryStatus.SUCCEEDED

    rows = await executor.fetch(job_id, max_rows=10)
    assert rows == [{"id": 1}]
