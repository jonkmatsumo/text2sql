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

    assert set(summary.keys()) == {"model", "benchmark", "drift", "feature_coverage", "config"}
    assert set(summary["model"].keys()) == {
        "state",
        "active_model_version",
        "last_reload_status",
        "last_reload_ts",
        "schema_mismatch_detected",
    }
    assert set(summary["benchmark"].keys()) == {"enabled", "last_status", "last_run_ts"}
    assert set(summary["drift"].keys()) == {
        "reference_resolution_mode",
        "last_error_code",
        "error_code",
        "error_message",
        "resolution_mode",
        "reference_model_version",
        "bucketing_requested",
        "bucketing_used",
    }
    assert set(summary["feature_coverage"].keys()) == {"last_ratio", "below_threshold"}
    assert set(summary["config"].keys()) == {
        "strict_feature_schema",
        "strict_tuning_resume_validation",
        "strict_split_strategy_validation",
        "strict_calibration_validation",
        "strict_schema_mismatch_blocking",
    }

    for section_name, section in summary.items():
        assert isinstance(section, dict), section_name
        for value in section.values():
            assert not isinstance(value, (list, dict))
            assert value is None or isinstance(value, (bool, int, float, str))

    assert isinstance(summary["model"]["schema_mismatch_detected"], bool)
    assert isinstance(summary["benchmark"]["enabled"], bool)
    assert summary["feature_coverage"]["last_ratio"] == 1.0
    assert summary["drift"]["last_error_code"] == "drift_fallback_disabled"
    assert summary["drift"]["error_code"] == "drift_fallback_disabled"
    assert summary["drift"]["error_message"] is None
    assert summary["drift"]["resolution_mode"] == "latest"


def test_operator_diagnostics_exposes_ml_health_shape():
    """Operator diagnostics should include a stable ml_health payload."""
    diagnostics = build_operator_diagnostics()
    ml_health = diagnostics["ml_health"]

    assert set(ml_health.keys()) == {"model", "benchmark", "drift", "feature_coverage", "config"}
    assert ml_health["benchmark"]["enabled"] is False


def test_operator_diagnostics_ml_health_config_defaults_false(monkeypatch):
    """Strict config booleans should remain false by default."""
    monkeypatch.delenv("MODEL_MANAGER_STRICT_RELOAD_MODE", raising=False)
    monkeypatch.delenv("MODEL_MANAGER_STRICT_SCHEMA_MODE", raising=False)
    monkeypatch.delenv("MODEL_MANAGER_CALIBRATION_STRICT_MODE", raising=False)
    monkeypatch.delenv("MODEL_MANAGER_DRIFT_STRICT_MODE", raising=False)
    monkeypatch.delenv("AGENT_BLOCK_ON_SCHEMA_MISMATCH", raising=False)

    diagnostics = build_operator_diagnostics()
    config = diagnostics["ml_health"]["config"]

    assert config["strict_feature_schema"] is False
    assert config["strict_tuning_resume_validation"] is False
    assert config["strict_split_strategy_validation"] is False
    assert config["strict_calibration_validation"] is False
    assert config["strict_schema_mismatch_blocking"] is False


def test_operator_diagnostics_ml_health_config_reflects_enabled_env(monkeypatch):
    """Strict config booleans should reflect effective env configuration."""
    monkeypatch.setenv("MODEL_MANAGER_STRICT_RELOAD_MODE", "true")
    monkeypatch.setenv("MODEL_MANAGER_STRICT_SCHEMA_MODE", "true")
    monkeypatch.setenv("MODEL_MANAGER_CALIBRATION_STRICT_MODE", "true")
    monkeypatch.setenv("MODEL_MANAGER_DRIFT_STRICT_MODE", "true")
    monkeypatch.setenv("AGENT_BLOCK_ON_SCHEMA_MISMATCH", "true")

    diagnostics = build_operator_diagnostics()
    config = diagnostics["ml_health"]["config"]

    assert config["strict_feature_schema"] is True
    assert config["strict_tuning_resume_validation"] is True
    assert config["strict_split_strategy_validation"] is True
    assert config["strict_calibration_validation"] is True
    assert config["strict_schema_mismatch_blocking"] is True


def test_ml_health_summary_optional_fields_use_null_convention():
    """Optional ml_health fields should be present and normalized to null when invalid."""
    summary = get_ml_health_summary(
        model_manager_snapshot={
            "state": " READY ",
            "last_reload_status": " WARN_ONLY ",
            "schema_mismatch_detected": {"unexpected": "map"},
        },
        benchmark_snapshot={"enabled": {"unexpected": True}, "last_status": ["bad"]},
        drift_snapshot={
            "error_code": "none",
            "error_message": "should be hidden for none",
            "reference_model_version": {"nested": "bad"},
            "bucketing_requested": {"not": "bool"},
            "bucketing_used": 1,
        },
        feature_coverage_snapshot={
            "last_ratio": {"invalid": "shape"},
            "below_threshold": ["not", "bool"],
        },
        strict_config={"strict_feature_schema": {"not": "bool"}},
    )

    assert set(summary.keys()) == {"model", "benchmark", "drift", "feature_coverage", "config"}
    assert summary["model"]["state"] == "ready"
    assert summary["model"]["last_reload_status"] == "warn_only"
    assert summary["model"]["schema_mismatch_detected"] is False
    assert summary["benchmark"]["enabled"] is False
    assert summary["benchmark"]["last_status"] is None
    assert summary["drift"]["error_code"] == "none"
    assert summary["drift"]["last_error_code"] == "none"
    assert summary["drift"]["error_message"] is None
    assert summary["drift"]["reference_model_version"] is None
    assert summary["drift"]["bucketing_requested"] is None
    assert summary["drift"]["bucketing_used"] is None
    assert summary["feature_coverage"]["last_ratio"] is None
    assert summary["feature_coverage"]["below_threshold"] is None
    assert summary["config"]["strict_feature_schema"] is False

    for section in summary.values():
        for value in section.values():
            assert value is None or isinstance(value, (bool, int, float, str))
            assert not isinstance(value, (list, dict))


def test_ml_health_summary_bounds_status_and_error_strings():
    """Status and error-like strings should be bounded and normalized."""
    summary = get_ml_health_summary(
        model_manager_snapshot={
            "state": "X" * 80,
            "last_reload_status": "Y" * 80,
            "active_model_version": "model-" + ("z" * 200),
        },
        benchmark_snapshot={"last_status": "PASS_" + ("A" * 200)},
        drift_snapshot={
            "error_code": "E" * 200,
            "error_message": "M" * 500,
            "resolution_mode": "STAGE",
            "reference_model_version": "ref-" + ("v" * 200),
        },
    )

    assert summary["model"]["state"] == ("x" * 32)
    assert summary["model"]["last_reload_status"] == ("y" * 32)
    assert len(summary["model"]["active_model_version"]) == 128
    assert summary["benchmark"]["last_status"] == ("pass_" + ("a" * 27))
    assert len(summary["drift"]["error_code"]) == 64
    assert len(summary["drift"]["last_error_code"]) == 64
    assert len(summary["drift"]["error_message"]) == 200
    assert summary["drift"]["resolution_mode"] == "stage"
    assert summary["drift"]["reference_resolution_mode"] == "stage"
    assert len(summary["drift"]["reference_model_version"]) == 128
