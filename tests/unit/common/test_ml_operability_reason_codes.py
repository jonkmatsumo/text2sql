"""Drift guards for ML-operability reason-code and metadata key vocabularies."""

from common.constants.ml_operability import (
    CALIBRATION_SKIP_REASON_ALLOWLIST,
    DRIFT_FALLBACK_REASON_ALLOWLIST,
    MLFLOW_AUDIT_PARAM_KEYS,
    MLFLOW_AUDIT_TAG_KEYS,
    MODEL_MANAGER_BASELINE_METADATA_KEYS,
    RELOAD_FAILURE_REASON_ALLOWLIST,
    SCHEMA_MISMATCH_REASON_ALLOWLIST,
)


def test_ml_operability_reason_code_allowlists_are_bounded_and_stable():
    """Reason-code sets should remain bounded and intentionally versioned."""
    assert RELOAD_FAILURE_REASON_ALLOWLIST == {
        "reload_exception",
        "build_pipeline_failed",
        "service_unavailable",
    }
    assert SCHEMA_MISMATCH_REASON_ALLOWLIST == {
        "schema_version_mismatch",
        "schema_missing_columns",
        "schema_missing_tables",
    }
    assert CALIBRATION_SKIP_REASON_ALLOWLIST == {
        "calibration_disabled",
        "insufficient_samples",
        "invalid_thresholds",
    }
    assert DRIFT_FALLBACK_REASON_ALLOWLIST == {
        "drift_fallback_disabled",
        "schema_snapshot_unavailable",
        "fallback_last_known_good",
    }


def test_ml_operability_metadata_keysets_are_bounded_and_stable():
    """Metadata key vocabularies must remain bounded to avoid cardinality drift."""
    assert MODEL_MANAGER_BASELINE_METADATA_KEYS == {
        "state",
        "active_model_version",
        "last_reload_status",
        "schema_mismatch_detected",
    }
    assert MLFLOW_AUDIT_TAG_KEYS == {
        "ml.audit.model_version",
        "ml.audit.reload_reason",
        "ml.audit.schema_mismatch_reason",
        "ml.audit.drift_fallback_reason",
    }
    assert MLFLOW_AUDIT_PARAM_KEYS == {
        "ml.audit.strict_schema_mode",
        "ml.audit.strict_reload_mode",
        "ml.audit.warn_only",
        "ml.audit.calibration_min_samples",
        "ml.audit.drift_threshold",
    }
