import hashlib
from unittest.mock import patch

import pytest
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from dal.tracing import TracedAsyncpgConnection, trace_enabled, trace_query_operation


class _DummyAsyncpgConn:
    async def execute(self, sql: str, *params: object) -> str:
        _ = (sql, params)
        return "OK"


def test_trace_enabled_defaults_true_when_otel_exporter_configured(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """DAL tracing should default to enabled when OTEL exporter is configured."""
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://otel-collector:4317")
    monkeypatch.delenv("DAL_TRACE_QUERIES", raising=False)

    assert trace_enabled() is True


def test_trace_enabled_respects_explicit_false_override(monkeypatch: pytest.MonkeyPatch) -> None:
    """Explicit DAL_TRACE_QUERIES=false should disable tracing despite exporter config."""
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://otel-collector:4317")
    monkeypatch.setenv("DAL_TRACE_QUERIES", "false")

    assert trace_enabled() is False


def test_trace_enabled_respects_explicit_true_override(monkeypatch: pytest.MonkeyPatch) -> None:
    """Explicit DAL_TRACE_QUERIES=true should keep tracing enabled."""
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://otel-collector:4317")
    monkeypatch.setenv("DAL_TRACE_QUERIES", "true")

    assert trace_enabled() is True


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


@pytest.mark.asyncio
async def test_trace_query_with_run_id(monkeypatch: pytest.MonkeyPatch) -> None:
    """Ensure run_id is attached to DAL spans when present in context."""
    from common.observability.context import run_id_var

    monkeypatch.setenv("DAL_TRACE_QUERIES", "true")

    # Set run_id context
    token = run_id_var.set("run-abc-123")

    try:
        exporter = InMemorySpanExporter()
        provider = TracerProvider()
        provider.add_span_processor(SimpleSpanProcessor(exporter))

        async def _op():
            return "ok"

        with patch("opentelemetry.trace.get_tracer") as mock_get_tracer:
            mock_get_tracer.side_effect = lambda name: provider.get_tracer(name)
            await trace_query_operation(
                "test", provider="pg", execution_model="sync", sql="sql", operation=_op()
            )

        spans = exporter.get_finished_spans()
        assert len(spans) == 1
        assert spans[0].attributes["run_id"] == "run-abc-123"

    finally:
        run_id_var.reset(token)
