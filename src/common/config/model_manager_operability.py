"""Model-manager operability flags and diagnostics snapshot helpers."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, FrozenSet

from common.config.env import get_env_bool
from common.constants.ml_operability import (
    CALIBRATION_SKIP_REASON_ALLOWLIST,
    DRIFT_FALLBACK_REASON_ALLOWLIST,
    MODEL_MANAGER_METADATA_KEY_ACTIVE_MODEL_VERSION,
    MODEL_MANAGER_METADATA_KEY_CALIBRATION_SKIP_REASON,
    MODEL_MANAGER_METADATA_KEY_DRIFT_FALLBACK_REASON,
    MODEL_MANAGER_METADATA_KEY_LAST_RELOAD_STATUS,
    MODEL_MANAGER_METADATA_KEY_RELOAD_FAILURE_REASON,
    MODEL_MANAGER_METADATA_KEY_SCHEMA_MISMATCH_DETECTED,
    MODEL_MANAGER_METADATA_KEY_SCHEMA_MISMATCH_REASON,
    MODEL_MANAGER_METADATA_KEY_STATE,
    RELOAD_FAILURE_REASON_ALLOWLIST,
    SCHEMA_MISMATCH_REASON_ALLOWLIST,
)


def _safe_env_bool(name: str, default: bool) -> bool:
    try:
        value = get_env_bool(name, default)
    except ValueError:
        return bool(default)
    if value is None:
        return bool(default)
    return bool(value)


def _bounded_reason_code(value: Any, allowlist: FrozenSet[str]) -> str | None:
    if not isinstance(value, str):
        return None
    normalized = value.strip().lower()
    if normalized in allowlist:
        return normalized
    return None


@dataclass(frozen=True)
class ModelManagerHardeningFlags:
    """Runtime hardening flags for model manager behavior."""

    strict_reload_mode: bool
    strict_schema_mode: bool
    schema_mismatch_warn_only: bool
    calibration_strict_mode: bool
    drift_strict_mode: bool

    @classmethod
    def from_env(cls) -> "ModelManagerHardeningFlags":
        """Load flags using fail-safe defaults (strict paths opt-in)."""
        return cls(
            strict_reload_mode=_safe_env_bool("MODEL_MANAGER_STRICT_RELOAD_MODE", False),
            strict_schema_mode=_safe_env_bool("MODEL_MANAGER_STRICT_SCHEMA_MODE", False),
            schema_mismatch_warn_only=_safe_env_bool(
                "MODEL_MANAGER_SCHEMA_MISMATCH_WARN_ONLY", True
            ),
            calibration_strict_mode=_safe_env_bool("MODEL_MANAGER_CALIBRATION_STRICT_MODE", False),
            drift_strict_mode=_safe_env_bool("MODEL_MANAGER_DRIFT_STRICT_MODE", False),
        )


def build_model_manager_diagnostics_snapshot(
    *,
    state: str = "idle",
    active_model_version: str = "unknown",
    last_reload_status: str = "not_loaded",
    schema_mismatch_detected: bool = False,
    reload_failure_reason: str | None = None,
    schema_mismatch_reason: str | None = None,
    calibration_skip_reason: str | None = None,
    drift_fallback_reason: str | None = None,
    flags: ModelManagerHardeningFlags | None = None,
) -> dict[str, Any]:
    """Build a bounded diagnostics snapshot with stable baseline keys."""
    active_flags = flags or ModelManagerHardeningFlags.from_env()
    normalized_state = str(state or "idle").strip().lower()
    normalized_reload_status = str(last_reload_status or "not_loaded").strip().lower()

    mismatch = bool(schema_mismatch_detected)
    if mismatch and not active_flags.strict_schema_mode and active_flags.schema_mismatch_warn_only:
        # Warn-only mode should keep mismatch non-fatal by default.
        normalized_reload_status = "warn_only"
        if normalized_state == "ready":
            normalized_state = "degraded"

    snapshot = {
        MODEL_MANAGER_METADATA_KEY_STATE: normalized_state or "idle",
        MODEL_MANAGER_METADATA_KEY_ACTIVE_MODEL_VERSION: str(active_model_version or "unknown"),
        MODEL_MANAGER_METADATA_KEY_LAST_RELOAD_STATUS: normalized_reload_status or "not_loaded",
        MODEL_MANAGER_METADATA_KEY_SCHEMA_MISMATCH_DETECTED: mismatch,
        MODEL_MANAGER_METADATA_KEY_RELOAD_FAILURE_REASON: _bounded_reason_code(
            reload_failure_reason, RELOAD_FAILURE_REASON_ALLOWLIST
        ),
        MODEL_MANAGER_METADATA_KEY_SCHEMA_MISMATCH_REASON: _bounded_reason_code(
            schema_mismatch_reason, SCHEMA_MISMATCH_REASON_ALLOWLIST
        ),
        MODEL_MANAGER_METADATA_KEY_CALIBRATION_SKIP_REASON: _bounded_reason_code(
            calibration_skip_reason, CALIBRATION_SKIP_REASON_ALLOWLIST
        ),
        MODEL_MANAGER_METADATA_KEY_DRIFT_FALLBACK_REASON: _bounded_reason_code(
            drift_fallback_reason, DRIFT_FALLBACK_REASON_ALLOWLIST
        ),
    }
    return snapshot
