"""Unit tests for operator diagnostics payload helpers."""

from agent.runtime_metrics import record_stage_latency_breakdown, reset_runtime_metrics
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


def test_build_operator_diagnostics_debug_includes_stage_latency_breakdown():
    """Debug mode should expose bounded stage-latency breakdown."""
    reset_runtime_metrics()
    record_stage_latency_breakdown(
        {
            "retrieval_ms": 11.0,
            "planning_ms": 22.0,
            "generation_ms": 33.0,
            "validation_ms": 44.0,
            "execution_ms": 55.0,
            "correction_loop_ms": 66.0,
        }
    )

    diagnostics = build_operator_diagnostics(debug=True)

    assert diagnostics["debug"]["latency_breakdown_ms"]["execution_ms"] == 55.0
