"""Tests for provider-aware retry classification."""

from agent.utils.retry import is_transient_error


def test_provider_specific_retryable(monkeypatch):
    """Provider-specific retryable errors should be detected."""
    monkeypatch.setenv("DAL_PROVIDER_AWARE_RETRY", "true")
    monkeypatch.setenv("QUERY_TARGET_BACKEND", "snowflake")
    exc = Exception("Warehouse is suspended")
    assert is_transient_error(exc) is True


def test_provider_specific_non_retryable(monkeypatch):
    """Provider-specific non-retryable errors should be detected."""
    monkeypatch.setenv("DAL_PROVIDER_AWARE_RETRY", "true")
    monkeypatch.setenv("QUERY_TARGET_BACKEND", "snowflake")
    exc = Exception("Unauthorized")
    assert is_transient_error(exc) is False
