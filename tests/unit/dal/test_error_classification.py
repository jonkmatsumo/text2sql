"""Unit tests for DAL error classification."""

import logging

from dal.error_classification import classify_error, emit_classified_error


class _BadRequest(Exception):
    __module__ = "google.api_core.exceptions"


class _PostgresSyntaxError(Exception):
    __module__ = "asyncpg.exceptions"


def test_classify_syntax_message() -> None:
    """Classify obvious SQL syntax errors."""
    exc = Exception('syntax error at or near "FROM"')
    assert classify_error("postgres", exc) == "syntax"


def test_classify_auth_message() -> None:
    """Classify permission errors."""
    exc = Exception("permission denied for relation foo")
    assert classify_error("postgres", exc) == "auth"


def test_classify_asyncpg_syntax_class() -> None:
    """Classify asyncpg syntax exceptions by class name."""
    exc = _PostgresSyntaxError("bad syntax")
    assert classify_error("postgres", exc) == "syntax"


def test_classify_bigquery_bad_request() -> None:
    """Classify BigQuery invalid query errors."""
    exc = _BadRequest("Invalid query")
    assert classify_error("bigquery", exc) == "syntax"


def test_emit_classified_error_logs_when_enabled(monkeypatch, caplog) -> None:
    """Structured telemetry is emitted when enabled."""
    monkeypatch.setenv("DAL_CLASSIFIED_ERROR_TELEMETRY", "true")
    with caplog.at_level(logging.ERROR):
        emit_classified_error("postgres", "execute_sql_query", "syntax", Exception("bad"))

    record = next(r for r in caplog.records if r.message == "dal_error_classified")
    assert record.provider == "postgres"
    assert record.operation == "execute_sql_query"
    assert record.error_category == "syntax"


def test_emit_classified_error_disabled(monkeypatch, caplog) -> None:
    """Structured telemetry is suppressed when disabled."""
    monkeypatch.delenv("DAL_CLASSIFIED_ERROR_TELEMETRY", raising=False)
    with caplog.at_level(logging.ERROR):
        emit_classified_error("postgres", "execute_sql_query", "syntax", Exception("bad"))

    assert not caplog.records
