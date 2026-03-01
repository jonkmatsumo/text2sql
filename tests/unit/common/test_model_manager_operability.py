"""Unit tests for model-manager operability flags and diagnostics snapshots."""

from common.config.diagnostics import build_operator_diagnostics
from common.config.model_manager_operability import (
    ModelManagerHardeningFlags,
    build_model_manager_diagnostics_snapshot,
)
from common.constants.ml_operability import MODEL_MANAGER_BASELINE_METADATA_KEYS


def test_model_manager_strict_flags_default_off(monkeypatch):
    """Strict model-manager hardening flags must remain opt-in by default."""
    monkeypatch.delenv("MODEL_MANAGER_STRICT_RELOAD_MODE", raising=False)
    monkeypatch.delenv("MODEL_MANAGER_STRICT_SCHEMA_MODE", raising=False)
    monkeypatch.delenv("MODEL_MANAGER_SCHEMA_MISMATCH_WARN_ONLY", raising=False)
    monkeypatch.delenv("MODEL_MANAGER_CALIBRATION_STRICT_MODE", raising=False)
    monkeypatch.delenv("MODEL_MANAGER_DRIFT_STRICT_MODE", raising=False)

    flags = ModelManagerHardeningFlags.from_env()

    assert flags.strict_reload_mode is False
    assert flags.strict_schema_mode is False
    assert flags.calibration_strict_mode is False
    assert flags.drift_strict_mode is False
    assert flags.schema_mismatch_warn_only is True


def test_model_manager_warn_only_mismatch_is_non_fatal():
    """Warn-only schema mismatch mode should not hard-fail diagnostics snapshot construction."""
    flags = ModelManagerHardeningFlags(
        strict_reload_mode=False,
        strict_schema_mode=False,
        schema_mismatch_warn_only=True,
        calibration_strict_mode=False,
        drift_strict_mode=False,
    )

    snapshot = build_model_manager_diagnostics_snapshot(
        state="ready",
        active_model_version="v1",
        last_reload_status="ok",
        schema_mismatch_detected=True,
        schema_mismatch_reason="schema_version_mismatch",
        flags=flags,
    )

    assert snapshot["state"] == "degraded"
    assert snapshot["active_model_version"] == "v1"
    assert snapshot["last_reload_status"] == "warn_only"
    assert snapshot["schema_mismatch_detected"] is True
    assert snapshot["schema_mismatch_reason"] == "schema_version_mismatch"


def test_operator_diagnostics_includes_model_manager_baseline_keys():
    """Diagnostics payload should always include baseline model-manager snapshot fields."""
    diagnostics = build_operator_diagnostics()

    model_manager = diagnostics["model_manager"]
    assert isinstance(model_manager, dict)
    assert MODEL_MANAGER_BASELINE_METADATA_KEYS.issubset(set(model_manager.keys()))
