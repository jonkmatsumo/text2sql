"""Unit tests for runtime configuration sanity validation."""

import pytest

from common.config.sanity import validate_runtime_configuration


def _clear_keys(monkeypatch, keys: list[str]) -> None:
    for key in keys:
        monkeypatch.delenv(key, raising=False)


def test_validate_runtime_configuration_allows_defaults(monkeypatch):
    """Default configuration should pass sanity validation."""
    _clear_keys(
        monkeypatch,
        [
            "AGENT_COLUMN_ALLOWLIST_MODE",
            "AGENT_CARTESIAN_JOIN_MODE",
            "AGENT_SCHEMA_BINDING_VALIDATION",
            "AGENT_SCHEMA_BINDING_SOFT_MODE",
            "AGENT_COLUMN_ALLOWLIST_FROM_SCHEMA_CONTEXT",
            "AGENT_COLUMN_ALLOWLIST",
            "AGENT_RETRY_POLICY",
            "AGENT_MAX_RETRIES",
            "SCHEMA_CACHE_TTL_SECONDS",
        ],
    )

    validate_runtime_configuration()


def test_validate_runtime_configuration_rejects_invalid_mode(monkeypatch):
    """Invalid enum values should fail with clear mode-specific messaging."""
    monkeypatch.setenv("AGENT_COLUMN_ALLOWLIST_MODE", "strict")

    with pytest.raises(RuntimeError) as exc_info:
        validate_runtime_configuration()

    assert "AGENT_COLUMN_ALLOWLIST_MODE" in str(exc_info.value)


def test_validate_runtime_configuration_rejects_soft_mode_without_binding(monkeypatch):
    """Soft schema binding mode should require schema binding validation enabled."""
    monkeypatch.setenv("AGENT_SCHEMA_BINDING_VALIDATION", "false")
    monkeypatch.setenv("AGENT_SCHEMA_BINDING_SOFT_MODE", "true")

    with pytest.raises(RuntimeError) as exc_info:
        validate_runtime_configuration()

    assert "AGENT_SCHEMA_BINDING_SOFT_MODE=true" in str(exc_info.value)


def test_validate_runtime_configuration_rejects_strict_column_blocking_without_context(monkeypatch):
    """Strict column allowlist blocking should require schema context or explicit allowlist."""
    monkeypatch.setenv("AGENT_COLUMN_ALLOWLIST_MODE", "block")
    monkeypatch.setenv("AGENT_COLUMN_ALLOWLIST_FROM_SCHEMA_CONTEXT", "false")
    monkeypatch.setenv("AGENT_COLUMN_ALLOWLIST", "")

    with pytest.raises(RuntimeError) as exc_info:
        validate_runtime_configuration()

    assert "AGENT_COLUMN_ALLOWLIST_MODE=block" in str(exc_info.value)


def test_validate_runtime_configuration_rejects_non_positive_ttl(monkeypatch):
    """Configured TTL values must be positive integers."""
    monkeypatch.setenv("SCHEMA_CACHE_TTL_SECONDS", "0")

    with pytest.raises(RuntimeError) as exc_info:
        validate_runtime_configuration()

    assert "SCHEMA_CACHE_TTL_SECONDS must be >= 1" in str(exc_info.value)
