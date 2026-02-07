"""Tests for ErrorMetadata and provider mapping."""

from common.models.error_metadata import ErrorMetadata
from dal.error_classification import extract_error_metadata


def test_error_metadata_model():
    """Test the ErrorMetadata model directly."""
    metadata = ErrorMetadata(
        provider="postgres",
        category="syntax",
        is_retryable=False,
        message='syntax error at or near "FROM"',
        sql_state="42601",
    )

    data = metadata.to_dict()
    assert data["provider"] == "postgres"
    assert data["category"] == "syntax"
    assert data["sql_state"] == "42601"
    assert "message" in data


def test_extract_error_metadata_generic(monkeypatch):
    """Test extracting metadata from a generic exception."""
    monkeypatch.setenv("DAL_EXPERIMENTAL_FEATURES", "on")
    exc = Exception("Something went wrong with password=12345")
    metadata = extract_error_metadata("postgres", exc)

    assert metadata.provider == "postgres"
    assert metadata.category == "unknown"
    assert "password=<redacted>" in metadata.message
    assert metadata.is_retryable is False


def test_extract_error_metadata_asyncpg(monkeypatch):
    """Test extracting metadata from an asyncpg-like exception."""
    monkeypatch.setenv("DAL_EXPERIMENTAL_FEATURES", "on")

    class MockPostgresError(Exception):
        def __init__(self, message, sqlstate, hint=None, position=None):
            super().__init__(message)
            self.sqlstate = sqlstate
            self.hint = hint
            self.position = position

    # Mocking an asyncpg-like error
    exc = MockPostgresError(
        'relation "users" does not exist',
        sqlstate="42P01",
        hint="Ensure the table name is correct.",
        position="15",
    )
    # Manually setting module to simulate asyncpg
    exc.__class__.__module__ = "asyncpg.exceptions"

    metadata = extract_error_metadata("postgres", exc)

    assert metadata.sql_state == "42P01"
    assert (
        metadata.category == "syntax"
    )  # 'does not exist' usually maps to syntax in our classification
    assert metadata.hint == "Ensure the table name is correct."
    assert metadata.position == 15
