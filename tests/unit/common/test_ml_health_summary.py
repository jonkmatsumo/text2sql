"""Unit tests for bounded ML health summary contracts."""

from common.config.diagnostics import build_operator_diagnostics
from common.config.model_manager_operability import get_ml_health_summary


def test_ml_health_summary_shape_is_stable_and_bounded():
    """Health summary should expose a stable, primitive-only contract."""
    summary = get_ml_health_summary(
        model_manager_snapshot={
            "state": "READY",
            "active_model_version": "v2026-03-02",
            "last_reload_status": "ok",
            "last_reload_ts": "2026-03-02T10:00:00Z",
            "schema_mismatch_detected": False,
            "drift_fallback_reason": "fallback_last_known_good",
        },
        benchmark_snapshot={
            "enabled": True,
            "last_status": "pass",
            "last_run_ts": "2026-03-02T10:05:00Z",
            "ignored_list": ["x", "y", "z"],
        },
        drift_snapshot={
            "reference_resolution_mode": "latest",
            "last_error_code": "drift_fallback_disabled",
            "ignored_map": {"foo": "bar"},
        },
        feature_coverage_snapshot={
            "last_ratio": 1.5,
            "below_threshold": True,
            "ignored_nested": {"bad": "shape"},
        },
    )

    assert set(summary.keys()) == {"model", "benchmark", "drift", "feature_coverage"}
    assert set(summary["model"].keys()) == {
        "state",
        "active_model_version",
        "last_reload_status",
        "last_reload_ts",
        "schema_mismatch_detected",
    }
    assert set(summary["benchmark"].keys()) == {"enabled", "last_status", "last_run_ts"}
    assert set(summary["drift"].keys()) == {"reference_resolution_mode", "last_error_code"}
    assert set(summary["feature_coverage"].keys()) == {"last_ratio", "below_threshold"}

    for section_name, section in summary.items():
        assert isinstance(section, dict), section_name
        for value in section.values():
            assert not isinstance(value, (list, dict))
            assert value is None or isinstance(value, (bool, int, float, str))

    assert isinstance(summary["model"]["schema_mismatch_detected"], bool)
    assert isinstance(summary["benchmark"]["enabled"], bool)
    assert summary["feature_coverage"]["last_ratio"] == 1.0
    assert summary["drift"]["last_error_code"] == "drift_fallback_disabled"


def test_operator_diagnostics_exposes_ml_health_shape():
    """Operator diagnostics should include a stable ml_health payload."""
    diagnostics = build_operator_diagnostics()
    ml_health = diagnostics["ml_health"]

    assert set(ml_health.keys()) == {"model", "benchmark", "drift", "feature_coverage"}
    assert ml_health["benchmark"]["enabled"] is False
