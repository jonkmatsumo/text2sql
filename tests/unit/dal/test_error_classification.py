import pytest

from common.models.error_metadata import ErrorCategory
from dal.error_classification import classify_error_info


@pytest.mark.parametrize(
    "provider, message, expected_category, is_retryable",
    [
        ("bigquery", "rate limit exceeded", ErrorCategory.THROTTLING, True),
        ("bigquery", "resources exceeded", ErrorCategory.RESOURCE_EXHAUSTED, True),
        ("snowflake", "warehouse sUspended", ErrorCategory.TRANSIENT, True),
        ("athena", "too many requests", ErrorCategory.THROTTLING, True),
        ("databricks", "temporarily unavailable", ErrorCategory.THROTTLING, True),
        ("unknown", "disk full", ErrorCategory.RESOURCE_EXHAUSTED, True),
        ("postgres", "deadlock detected", ErrorCategory.DEADLOCK, True),
    ],
)
def test_error_classification_unification(provider, message, expected_category, is_retryable):
    """Verify that errors map to unified categories."""
    exc = Exception(message)
    info = classify_error_info(provider, exc)
    assert info.category == expected_category
    assert info.provider == provider
    assert info.is_retryable == is_retryable
