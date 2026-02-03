import hashlib
from unittest.mock import patch

import pytest
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from dal.tracing import TracedAsyncpgConnection, trace_query_operation


class _DummyAsyncpgConn:
    async def execute(self, sql: str, *params: object) -> str:
        _ = (sql, params)
        return "OK"


@pytest.mark.asyncio
async def test_trace_execute_span(monkeypatch: pytest.MonkeyPatch) -> None:
    """Ensure sync query tracing emits a span with hashed SQL."""
    monkeypatch.setenv("DAL_TRACE_QUERIES", "true")
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))

    with patch("opentelemetry.trace.get_tracer") as mock_get_tracer:
        mock_get_tracer.side_effect = lambda name: provider.get_tracer(name)
        conn = TracedAsyncpgConnection(_DummyAsyncpgConn(), "postgres", "sync")
        result = await conn.execute("select 1", 1)

    assert result == "OK"
    spans = exporter.get_finished_spans()
    assert len(spans) == 1
    span = spans[0]
    assert span.name == "dal.query.execute"
    assert span.attributes["db.provider"] == "postgres"
    assert span.attributes["db.execution_model"] == "sync"
    assert (
        span.attributes["db.statement_hash"]
        == hashlib.sha256("select 1".encode("utf-8")).hexdigest()
    )
    assert span.attributes["db.status"] == "ok"
    assert "db.statement" not in span.attributes


@pytest.mark.asyncio
async def test_trace_submit_span(monkeypatch: pytest.MonkeyPatch) -> None:
    """Ensure async query tracing emits a span with hashed SQL."""
    monkeypatch.setenv("DAL_TRACE_QUERIES", "true")
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))

    async def _operation() -> str:
        return "job-1"

    with patch("opentelemetry.trace.get_tracer") as mock_get_tracer:
        mock_get_tracer.side_effect = lambda name: provider.get_tracer(name)
        result = await trace_query_operation(
            "dal.query.submit",
            provider="snowflake",
            execution_model="async",
            sql="select 2",
            operation=_operation(),
        )

    assert result == "job-1"
    spans = exporter.get_finished_spans()
    assert len(spans) == 1
    span = spans[0]
    assert span.name == "dal.query.submit"
    assert span.attributes["db.provider"] == "snowflake"
    assert span.attributes["db.execution_model"] == "async"
    assert (
        span.attributes["db.statement_hash"]
        == hashlib.sha256("select 2".encode("utf-8")).hexdigest()
    )
    assert span.attributes["db.status"] == "ok"
    assert "db.statement" not in span.attributes
