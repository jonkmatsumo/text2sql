"""Golden contract-shape tests for ML health and drift payloads."""

from agent.utils.drift_detection import detect_schema_drift_details
from common.config.diagnostics import build_operator_diagnostics


def test_diagnostics_ml_health_contract_shape():
    """Diagnostics ml_health payload should keep stable keys and primitive values."""
    ml_health = build_operator_diagnostics()["ml_health"]

    assert set(ml_health.keys()) == {"model", "benchmark", "drift", "feature_coverage", "config"}
    assert set(ml_health["model"].keys()) == {
        "state",
        "active_model_version",
        "last_reload_status",
        "last_reload_ts",
        "schema_mismatch_detected",
    }
    assert set(ml_health["benchmark"].keys()) == {"enabled", "last_status", "last_run_ts"}
    assert set(ml_health["drift"].keys()) == {
        "reference_resolution_mode",
        "last_error_code",
        "error_code",
        "error_message",
        "resolution_mode",
        "reference_model_version",
        "bucketing_requested",
        "bucketing_used",
    }
    assert set(ml_health["feature_coverage"].keys()) == {"last_ratio", "below_threshold"}
    assert set(ml_health["config"].keys()) == {
        "strict_feature_schema",
        "strict_tuning_resume_validation",
        "strict_split_strategy_validation",
        "strict_calibration_validation",
        "strict_schema_mismatch_blocking",
    }

    for section in ml_health.values():
        assert isinstance(section, dict)
        for value in section.values():
            assert not isinstance(value, (list, dict))
            assert value is None or isinstance(value, (bool, int, float, str))


def test_drift_contract_shape_and_boundedness():
    """Drift output payload should retain stable keys and bounded field sizes."""
    result = detect_schema_drift_details(
        sql="INVALID SQL !!!",
        error_message="PSI suppressed due to sparse buckets " + ("x" * 500),
        provider="postgres",
        raw_schema_context=[],
        error_metadata={
            "drift_error_code": "psi_sparse_buckets",
            "resolution_mode": "stage",
            "reference_model_version": "model-v3",
            "bucketing_requested": True,
            "bucketing_used": False,
        },
    ).to_dict()

    assert set(result.keys()) == {
        "missing_identifiers",
        "method",
        "source",
        "error_code",
        "error_message",
        "resolution_mode",
        "reference_model_version",
        "bucketing_requested",
        "bucketing_used",
    }
    assert result["error_code"] == "psi_sparse_buckets"
    assert result["resolution_mode"] == "stage"
    assert result["reference_model_version"] == "model-v3"

    assert isinstance(result["missing_identifiers"], list)
    assert len(result["missing_identifiers"]) <= 20
    assert all(isinstance(identifier, str) for identifier in result["missing_identifiers"])
    assert isinstance(result["error_message"], str)
    assert len(result["error_message"]) <= 200
