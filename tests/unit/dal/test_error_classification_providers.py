"""Tests for provider-specific error classification patterns."""

import pytest

from dal.error_classification import classify_error


class MockError(Exception):
    """Mock exception for testing."""

    pass


@pytest.mark.parametrize(
    "provider, message, expected_category",
    [
        # Auth / Permission
        ("postgres", "ERROR: permission denied for table users", "auth"),
        ("bigquery", "Access Denied: User does not have permission", "auth"),
        ("snowflake", "Insufficient privileges to operate on schema", "auth"),
        # Timeout
        ("postgres", "canceling statement due to statement_timeout", "timeout"),
        ("redshift", "Query execution limit exceeded", "timeout"),
        ("athena", "Query timeout", "timeout"),
        # Quota / Throttling
        ("bigquery", "Quota exceeded: Your project exceeded quota for imports", "throttling"),
        ("snowflake", "Concurrency limit reached", "throttling"),
        ("generic", "Too many requests", "throttling"),
        # Resource
        ("postgres", "disk is full", "resource_exhausted"),  # Maybe?
        ("bigquery", "Resources exceeded during query execution", "resource_exhausted"),
    ],
)
def test_classify_provider_errors(provider, message, expected_category):
    """Test classification of various provider error messages."""
    err = MockError(message)
    category = classify_error(provider, err)
    # We allow some flexibility if it falls back to broader category,
    # but for these we expect specific mapping.
    assert category == expected_category
