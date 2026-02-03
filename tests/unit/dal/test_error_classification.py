"""Unit tests for DAL error classification."""

from dal.error_classification import classify_error


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
