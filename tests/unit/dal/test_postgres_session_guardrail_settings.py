"""Unit tests for Postgres session guardrail startup settings."""

import pytest

from dal.database import Database
from dal.session_guardrails import PostgresSessionGuardrailSettings


@pytest.fixture(autouse=True)
def _reset_database_provider_state():
    Database._query_target_provider = "postgres"
    Database._postgres_session_guardrail_settings = None
    yield
    Database._query_target_provider = "postgres"
    Database._postgres_session_guardrail_settings = None


def test_postgres_session_guardrail_settings_from_env_defaults(monkeypatch):
    """Guardrail settings should default to a fully disabled posture."""
    monkeypatch.delenv("POSTGRES_RESTRICTED_SESSION_ENABLED", raising=False)
    monkeypatch.delenv("POSTGRES_EXECUTION_ROLE_ENABLED", raising=False)
    monkeypatch.delenv("POSTGRES_EXECUTION_ROLE", raising=False)
    settings = PostgresSessionGuardrailSettings.from_env()
    assert settings.restricted_session_enabled is False
    assert settings.execution_role_enabled is False
    assert settings.execution_role_name is None


def test_postgres_session_guardrail_settings_missing_execution_role_fails():
    """Execution role enablement without a role should fail closed."""
    settings = PostgresSessionGuardrailSettings(
        restricted_session_enabled=False,
        execution_role_enabled=True,
        execution_role_name=None,
    )
    with pytest.raises(ValueError, match="POSTGRES_EXECUTION_ROLE"):
        settings.validate_basic("postgres")


@pytest.mark.parametrize(
    "restricted_enabled, role_enabled",
    [
        (True, False),
        (False, True),
    ],
)
def test_postgres_session_guardrail_settings_non_postgres_provider_fails(
    restricted_enabled: bool, role_enabled: bool
):
    """Guardrails enabled under non-postgres providers should fail closed."""
    settings = PostgresSessionGuardrailSettings(
        restricted_session_enabled=restricted_enabled,
        execution_role_enabled=role_enabled,
        execution_role_name="text2sql_readonly",
    )
    with pytest.raises(ValueError, match="provider=postgres"):
        settings.validate_basic("sqlite")


def test_database_load_postgres_session_guardrail_settings_fails_closed(monkeypatch):
    """Database startup loader should reject invalid guardrail env combinations."""
    Database._query_target_provider = "postgres"
    monkeypatch.setenv("POSTGRES_EXECUTION_ROLE_ENABLED", "true")
    monkeypatch.delenv("POSTGRES_EXECUTION_ROLE", raising=False)

    with pytest.raises(ValueError, match="POSTGRES_EXECUTION_ROLE"):
        Database._load_postgres_session_guardrail_settings()


def test_database_load_postgres_session_guardrail_settings_from_env(monkeypatch):
    """Database startup loader should persist validated guardrail settings."""
    Database._query_target_provider = "postgres"
    monkeypatch.setenv("POSTGRES_RESTRICTED_SESSION_ENABLED", "true")
    monkeypatch.setenv("POSTGRES_EXECUTION_ROLE_ENABLED", "true")
    monkeypatch.setenv("POSTGRES_EXECUTION_ROLE", "text2sql_readonly")

    settings = Database._load_postgres_session_guardrail_settings()

    assert settings.restricted_session_enabled is True
    assert settings.execution_role_enabled is True
    assert settings.execution_role_name == "text2sql_readonly"
    assert Database._postgres_session_guardrail_settings == settings
