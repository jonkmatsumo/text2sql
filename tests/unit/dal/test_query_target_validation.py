import pytest

from dal.query_target_validation import QueryTargetValidationError, validate_query_target_payload


def test_validation_rejects_secret_fields():
    """Reject secret-like keys in payload."""
    with pytest.raises(QueryTargetValidationError):
        validate_query_target_payload(
            provider="postgres",
            metadata={"host": "db", "db_name": "app", "user": "ro", "password": "nope"},
            auth={},
            guardrails={},
        )


def test_validation_rejects_disallowed_auth_keys():
    """Reject auth keys outside the allowlist."""
    with pytest.raises(QueryTargetValidationError):
        validate_query_target_payload(
            provider="mysql",
            metadata={"host": "db", "db_name": "app", "user": "ro"},
            auth={"token": "secret"},
            guardrails={},
        )


def test_validation_requires_fields():
    """Reject missing required fields."""
    with pytest.raises(QueryTargetValidationError):
        validate_query_target_payload(
            provider="postgres",
            metadata={"host": "db", "db_name": "app"},
            auth={},
            guardrails={},
        )


def test_validation_blocks_local_providers_without_flag(monkeypatch):
    """Require opt-in for local-only providers."""
    monkeypatch.delenv("DAL_ALLOW_LOCAL_QUERY_TARGETS", raising=False)
    with pytest.raises(QueryTargetValidationError):
        validate_query_target_payload(
            provider="sqlite",
            metadata={"path": "/tmp/test.db"},
            auth={},
            guardrails={},
        )


def test_validation_allows_local_providers_with_flag(monkeypatch):
    """Allow local-only providers when opt-in flag is set."""
    monkeypatch.setenv("DAL_ALLOW_LOCAL_QUERY_TARGETS", "true")
    metadata, auth, guardrails = validate_query_target_payload(
        provider="duckdb",
        metadata={"path": ":memory:"},
        auth={},
        guardrails={"read_only": False},
    )
    assert metadata["path"] == ":memory:"
    assert guardrails["read_only"] is False


def test_validation_rejects_bad_guardrail_types():
    """Reject invalid guardrail types/values."""
    with pytest.raises(QueryTargetValidationError):
        validate_query_target_payload(
            provider="clickhouse",
            metadata={"host": "db", "database": "default"},
            auth={},
            guardrails={"max_rows": -1},
        )
