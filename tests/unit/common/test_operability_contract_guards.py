"""Compatibility guard tests for ML health and drift contracts."""

from __future__ import annotations

from typing import Any

import pytest

from agent.utils.drift_detection import detect_schema_drift_details
from common.config.diagnostics import build_operator_diagnostics
from common.config.model_manager_operability import get_ml_health_summary

_ML_HEALTH_TOP_LEVEL_KEYS = {"model", "benchmark", "drift", "feature_coverage", "config"}
_ML_HEALTH_MODEL_KEYS = {
    "state",
    "active_model_version",
    "last_reload_status",
    "last_reload_ts",
    "schema_mismatch_detected",
}
_ML_HEALTH_BENCHMARK_KEYS = {"enabled", "last_status", "last_run_ts"}
_ML_HEALTH_DRIFT_KEYS = {
    "reference_resolution_mode",
    "last_error_code",
    "error_code",
    "error_message",
    "resolution_mode",
    "reference_model_version",
    "bucketing_requested",
    "bucketing_used",
}
_ML_HEALTH_FEATURE_COVERAGE_KEYS = {"last_ratio", "below_threshold"}
_ML_HEALTH_CONFIG_KEYS = {
    "strict_feature_schema",
    "strict_tuning_resume_validation",
    "strict_split_strategy_validation",
    "strict_calibration_validation",
    "strict_schema_mismatch_blocking",
}
_DRIFT_RESULT_KEYS = {
    "missing_identifiers",
    "method",
    "source",
    "last_error_code",
    "error_code",
    "error_message",
    "reference_resolution_mode",
    "resolution_mode",
    "reference_model_version",
    "reference_available",
    "reference_selection_source",
    "bucketing_requested",
    "bucketing_used",
}


def _assert_optional_primitive(value: Any, allowed_types: tuple[type, ...]) -> None:
    assert value is None or isinstance(value, allowed_types)


def _assert_optional_bounded_str(value: Any, *, max_length: int) -> None:
    _assert_optional_primitive(value, (str,))
    if isinstance(value, str):
        assert len(value) <= max_length


def test_ml_health_contract_guard_required_optional_types_and_bounds():
    """Guard diagnostics.ml_health shape, types, and bounded strings."""
    diagnostics = build_operator_diagnostics()
    ml_health = diagnostics["ml_health"]

    assert set(ml_health.keys()) == _ML_HEALTH_TOP_LEVEL_KEYS
    assert set(ml_health["model"].keys()) == _ML_HEALTH_MODEL_KEYS
    assert set(ml_health["benchmark"].keys()) == _ML_HEALTH_BENCHMARK_KEYS
    assert set(ml_health["drift"].keys()) == _ML_HEALTH_DRIFT_KEYS
    assert set(ml_health["feature_coverage"].keys()) == _ML_HEALTH_FEATURE_COVERAGE_KEYS
    assert set(ml_health["config"].keys()) == _ML_HEALTH_CONFIG_KEYS

    assert isinstance(ml_health["model"]["state"], str)
    assert isinstance(ml_health["model"]["active_model_version"], str)
    assert isinstance(ml_health["model"]["last_reload_status"], str)
    assert isinstance(ml_health["model"]["schema_mismatch_detected"], bool)
    _assert_optional_primitive(ml_health["model"]["last_reload_ts"], (str,))

    assert isinstance(ml_health["benchmark"]["enabled"], bool)
    _assert_optional_primitive(ml_health["benchmark"]["last_status"], (str,))
    _assert_optional_primitive(ml_health["benchmark"]["last_run_ts"], (str,))

    _assert_optional_bounded_str(ml_health["drift"]["error_code"], max_length=64)
    _assert_optional_bounded_str(ml_health["drift"]["last_error_code"], max_length=64)
    _assert_optional_bounded_str(ml_health["drift"]["error_message"], max_length=200)
    _assert_optional_bounded_str(ml_health["drift"]["reference_model_version"], max_length=128)
    assert isinstance(ml_health["drift"]["resolution_mode"], str)
    assert isinstance(ml_health["drift"]["reference_resolution_mode"], str)
    _assert_optional_primitive(ml_health["drift"]["bucketing_requested"], (bool,))
    _assert_optional_primitive(ml_health["drift"]["bucketing_used"], (bool,))
    assert ml_health["drift"]["last_error_code"] == ml_health["drift"]["error_code"]
    assert ml_health["drift"]["reference_resolution_mode"] == ml_health["drift"]["resolution_mode"]

    _assert_optional_primitive(ml_health["feature_coverage"]["last_ratio"], (int, float))
    _assert_optional_primitive(ml_health["feature_coverage"]["below_threshold"], (bool,))

    for value in ml_health["config"].values():
        assert isinstance(value, bool)

    summary = get_ml_health_summary(
        drift_snapshot={
            "error_code": "E" * 500,
            "error_message": "M" * 500,
            "reference_model_version": "V" * 500,
        }
    )
    assert len(summary["drift"]["error_code"]) == 64
    assert len(summary["drift"]["error_message"]) == 200
    assert len(summary["drift"]["reference_model_version"]) == 128


@pytest.mark.parametrize(
    ("name", "kwargs", "expected_error_code", "expected_reference_available"),
    [
        (
            "success",
            {
                "sql": "SELECT name FROM users",
                "error_message": "",
                "provider": "postgres",
                "raw_schema_context": [{"type": "Table", "name": "users"}],
                "error_metadata": None,
            },
            None,
            True,
        ),
        (
            "no_reference",
            {
                "sql": "SELECT * FROM users",
                "error_message": "Reference model unavailable for drift check",
                "provider": "postgres",
                "raw_schema_context": [],
                "error_metadata": None,
            },
            "no_reference_model",
            False,
        ),
        (
            "insufficient_samples",
            {
                "sql": "SELECT email FROM users",
                "error_message": "Reference sample count below minimum",
                "provider": "postgres",
                "raw_schema_context": [{"type": "Table", "name": "users"}],
                "error_metadata": {
                    "drift_error_code": "insufficient_reference_samples",
                    "reference_resolution_mode": "stage",
                    "reference_model_version": "model-v7",
                    "bucketing_requested": True,
                    "bucketing_used": False,
                },
            },
            "insufficient_reference_samples",
            True,
        ),
        (
            "sparse_buckets_suppressed",
            {
                "sql": "SELECT id FROM users",
                "error_message": "PSI suppressed due to sparse buckets in reference histogram",
                "provider": "postgres",
                "raw_schema_context": [{"type": "Table", "name": "users"}],
                "error_metadata": {"resolution_mode": "alias"},
            },
            "psi_sparse_buckets",
            True,
        ),
    ],
)
def test_drift_contract_guard_shape_and_bounds_across_modes(
    name: str,
    kwargs: dict[str, Any],
    expected_error_code: str | None,
    expected_reference_available: bool,
):
    """Guard drift payload shape across success/failure result modes."""
    result = detect_schema_drift_details(**kwargs).to_dict()

    assert set(result.keys()) == _DRIFT_RESULT_KEYS, name
    assert isinstance(result["missing_identifiers"], list)
    assert isinstance(result["method"], str)
    assert isinstance(result["source"], str)
    assert result["error_code"] == expected_error_code
    assert result["last_error_code"] == expected_error_code
    _assert_optional_bounded_str(result["error_message"], max_length=200)
    assert isinstance(result["resolution_mode"], str)
    assert isinstance(result["reference_resolution_mode"], str)
    assert result["reference_resolution_mode"] == result["resolution_mode"]
    _assert_optional_bounded_str(result["reference_model_version"], max_length=128)
    assert result["reference_available"] is expected_reference_available
    assert result["reference_selection_source"] in {"alias", "stage", "latest", "none"}
    _assert_optional_primitive(result["bucketing_requested"], (bool,))
    _assert_optional_primitive(result["bucketing_used"], (bool,))
