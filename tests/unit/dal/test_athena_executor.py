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
