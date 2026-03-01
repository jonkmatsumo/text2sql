"""Canonical constants for ML operability diagnostics and bounded reason codes."""

from __future__ import annotations

from typing import Final, FrozenSet

# Diagnostics metadata keys (stable contract surface)
MODEL_MANAGER_METADATA_KEY_STATE: Final[str] = "state"
MODEL_MANAGER_METADATA_KEY_ACTIVE_MODEL_VERSION: Final[str] = "active_model_version"
MODEL_MANAGER_METADATA_KEY_LAST_RELOAD_STATUS: Final[str] = "last_reload_status"
MODEL_MANAGER_METADATA_KEY_SCHEMA_MISMATCH_DETECTED: Final[str] = "schema_mismatch_detected"
MODEL_MANAGER_METADATA_KEY_RELOAD_FAILURE_REASON: Final[str] = "reload_failure_reason"
MODEL_MANAGER_METADATA_KEY_SCHEMA_MISMATCH_REASON: Final[str] = "schema_mismatch_reason"
MODEL_MANAGER_METADATA_KEY_CALIBRATION_SKIP_REASON: Final[str] = "calibration_skip_reason"
MODEL_MANAGER_METADATA_KEY_DRIFT_FALLBACK_REASON: Final[str] = "drift_fallback_reason"

MODEL_MANAGER_BASELINE_METADATA_KEYS: Final[FrozenSet[str]] = frozenset(
    {
        MODEL_MANAGER_METADATA_KEY_STATE,
        MODEL_MANAGER_METADATA_KEY_ACTIVE_MODEL_VERSION,
        MODEL_MANAGER_METADATA_KEY_LAST_RELOAD_STATUS,
        MODEL_MANAGER_METADATA_KEY_SCHEMA_MISMATCH_DETECTED,
    }
)

# Reload failure reason codes
RELOAD_FAILURE_REASON_RELOAD_EXCEPTION: Final[str] = "reload_exception"
RELOAD_FAILURE_REASON_BUILD_PIPELINE_FAILED: Final[str] = "build_pipeline_failed"
RELOAD_FAILURE_REASON_SERVICE_UNAVAILABLE: Final[str] = "service_unavailable"

RELOAD_FAILURE_REASON_ALLOWLIST: Final[FrozenSet[str]] = frozenset(
    {
        RELOAD_FAILURE_REASON_RELOAD_EXCEPTION,
        RELOAD_FAILURE_REASON_BUILD_PIPELINE_FAILED,
        RELOAD_FAILURE_REASON_SERVICE_UNAVAILABLE,
    }
)

# Schema mismatch reason codes
SCHEMA_MISMATCH_REASON_VERSION_MISMATCH: Final[str] = "schema_version_mismatch"
SCHEMA_MISMATCH_REASON_MISSING_COLUMNS: Final[str] = "schema_missing_columns"
SCHEMA_MISMATCH_REASON_MISSING_TABLES: Final[str] = "schema_missing_tables"

SCHEMA_MISMATCH_REASON_ALLOWLIST: Final[FrozenSet[str]] = frozenset(
    {
        SCHEMA_MISMATCH_REASON_VERSION_MISMATCH,
        SCHEMA_MISMATCH_REASON_MISSING_COLUMNS,
        SCHEMA_MISMATCH_REASON_MISSING_TABLES,
    }
)

# Calibration skip reason codes
CALIBRATION_SKIP_REASON_DISABLED: Final[str] = "calibration_disabled"
CALIBRATION_SKIP_REASON_INSUFFICIENT_SAMPLES: Final[str] = "insufficient_samples"
CALIBRATION_SKIP_REASON_INVALID_THRESHOLDS: Final[str] = "invalid_thresholds"

CALIBRATION_SKIP_REASON_ALLOWLIST: Final[FrozenSet[str]] = frozenset(
    {
        CALIBRATION_SKIP_REASON_DISABLED,
        CALIBRATION_SKIP_REASON_INSUFFICIENT_SAMPLES,
        CALIBRATION_SKIP_REASON_INVALID_THRESHOLDS,
    }
)

# Drift fallback reason codes
DRIFT_FALLBACK_REASON_DISABLED: Final[str] = "drift_fallback_disabled"
DRIFT_FALLBACK_REASON_SCHEMA_UNAVAILABLE: Final[str] = "schema_snapshot_unavailable"
DRIFT_FALLBACK_REASON_LAST_KNOWN_GOOD: Final[str] = "fallback_last_known_good"

DRIFT_FALLBACK_REASON_ALLOWLIST: Final[FrozenSet[str]] = frozenset(
    {
        DRIFT_FALLBACK_REASON_DISABLED,
        DRIFT_FALLBACK_REASON_SCHEMA_UNAVAILABLE,
        DRIFT_FALLBACK_REASON_LAST_KNOWN_GOOD,
    }
)

# MLflow audit keys (bounded keyspace)
MLFLOW_AUDIT_TAG_MODEL_VERSION: Final[str] = "ml.audit.model_version"
MLFLOW_AUDIT_TAG_RELOAD_REASON: Final[str] = "ml.audit.reload_reason"
MLFLOW_AUDIT_TAG_SCHEMA_MISMATCH_REASON: Final[str] = "ml.audit.schema_mismatch_reason"
MLFLOW_AUDIT_TAG_DRIFT_FALLBACK_REASON: Final[str] = "ml.audit.drift_fallback_reason"

MLFLOW_AUDIT_TAG_KEYS: Final[FrozenSet[str]] = frozenset(
    {
        MLFLOW_AUDIT_TAG_MODEL_VERSION,
        MLFLOW_AUDIT_TAG_RELOAD_REASON,
        MLFLOW_AUDIT_TAG_SCHEMA_MISMATCH_REASON,
        MLFLOW_AUDIT_TAG_DRIFT_FALLBACK_REASON,
    }
)

MLFLOW_AUDIT_PARAM_STRICT_SCHEMA_MODE: Final[str] = "ml.audit.strict_schema_mode"
MLFLOW_AUDIT_PARAM_STRICT_RELOAD_MODE: Final[str] = "ml.audit.strict_reload_mode"
MLFLOW_AUDIT_PARAM_WARN_ONLY_MODE: Final[str] = "ml.audit.warn_only"
MLFLOW_AUDIT_PARAM_CALIBRATION_MIN_SAMPLES: Final[str] = "ml.audit.calibration_min_samples"
MLFLOW_AUDIT_PARAM_DRIFT_THRESHOLD: Final[str] = "ml.audit.drift_threshold"

MLFLOW_AUDIT_PARAM_KEYS: Final[FrozenSet[str]] = frozenset(
    {
        MLFLOW_AUDIT_PARAM_STRICT_SCHEMA_MODE,
        MLFLOW_AUDIT_PARAM_STRICT_RELOAD_MODE,
        MLFLOW_AUDIT_PARAM_WARN_ONLY_MODE,
        MLFLOW_AUDIT_PARAM_CALIBRATION_MIN_SAMPLES,
        MLFLOW_AUDIT_PARAM_DRIFT_THRESHOLD,
    }
)
