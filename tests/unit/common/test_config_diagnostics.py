"""Unit tests for operator diagnostics payload helpers."""

from agent.runtime_metrics import (
    record_query_complexity_score,
    record_stage_latency_breakdown,
    record_truncation_event,
    reset_runtime_metrics,
)
from agent.utils.schema_cache import reset_schema_cache, set_cached_schema_snapshot_id
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
    assert diagnostics["runtime_indicators"]["active_schema_cache_size"] >= 0
    assert diagnostics["runtime_indicators"]["avg_query_complexity"] >= 0.0
    assert diagnostics["runtime_indicators"]["recent_truncation_event_count"] >= 0
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


def test_build_operator_diagnostics_runtime_indicators_include_recent_metrics():
    """Runtime indicators should expose bounded aggregate health signals."""
    reset_runtime_metrics()
    reset_schema_cache()
    set_cached_schema_snapshot_id(tenant_id=1, snapshot_id="fp-1")
    record_query_complexity_score(10)
    record_query_complexity_score(20)
    record_truncation_event(True)
    record_truncation_event(False)

    diagnostics = build_operator_diagnostics()
    runtime = diagnostics["runtime_indicators"]

    assert runtime["active_schema_cache_size"] == 1
    assert runtime["avg_query_complexity"] == 15.0
    assert runtime["recent_truncation_event_count"] == 1
    assert runtime["last_schema_refresh_timestamp"] is not None
