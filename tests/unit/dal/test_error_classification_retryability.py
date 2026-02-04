"""Tests for provider-aware error classification retryability."""

from dal.error_classification import classify_error_info


def test_postgres_deadlock_retryable():
    """Deadlock errors should be retryable for Postgres."""
    exc = Exception("deadlock detected")
    info = classify_error_info("postgres", exc)
    assert info.category == "deadlock"
    assert info.is_retryable is True


def test_postgres_serialization_retryable():
    """Serialization errors should be retryable for Postgres."""
    exc = Exception("could not serialize access due to concurrent update")
    info = classify_error_info("postgres", exc)
    assert info.category == "serialization"
    assert info.is_retryable is True


def test_bigquery_quota_retryable():
    """Verify BigQuery quota errors are retryable."""
    exc = Exception("Quota exceeded for quota group")
    info = classify_error_info("bigquery", exc)
    assert info.category == "throttling"
    assert info.is_retryable is True


def test_snowflake_warehouse_suspended_retryable():
    """Snowflake suspended warehouses should be retryable."""
    exc = Exception("Warehouse is suspended")
    info = classify_error_info("snowflake", exc)
    assert info.category == "transient"
    assert info.is_retryable is True


def test_generic_timeout_retryable():
    """Timeout errors should be retryable."""
    exc = TimeoutError("timed out")
    info = classify_error_info("unknown", exc)
    assert info.category == "timeout"
    assert info.is_retryable is True
