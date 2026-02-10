"""Unit tests for operator diagnostics payload helpers."""

from common.config.diagnostics import build_operator_diagnostics


def test_build_operator_diagnostics_returns_expected_shape(monkeypatch):
    """Diagnostics payload should include stable top-level operator fields."""
    monkeypatch.setenv("QUERY_TARGET_BACKEND", "postgres")
    monkeypatch.setenv("AGENT_RETRY_POLICY", "adaptive")
    monkeypatch.setenv("AGENT_MAX_RETRIES", "4")
    monkeypatch.setenv("SCHEMA_CACHE_TTL_SECONDS", "1200")

    diagnostics = build_operator_diagnostics()

    assert diagnostics["active_database_provider"] == "postgres"
    assert diagnostics["retry_policy"]["mode"] == "adaptive"
    assert diagnostics["retry_policy"]["max_retries"] == 4
    assert diagnostics["schema_cache_ttl_seconds"] == 1200
    assert "enabled_flags" in diagnostics
    assert diagnostics["enabled_flags"]["column_allowlist_mode"] in {"warn", "block", "off"}


def test_build_operator_diagnostics_falls_back_on_invalid_ints(monkeypatch):
    """Invalid numeric env vars should fall back to safe defaults."""
    monkeypatch.setenv("AGENT_MAX_RETRIES", "not-a-number")

    diagnostics = build_operator_diagnostics()

    assert diagnostics["retry_policy"]["max_retries"] == 3
