"""Operator-safe runtime diagnostics for Text2SQL services."""

from __future__ import annotations

from typing import Any

from common.config.env import get_env_bool, get_env_int, get_env_str
from common.config.model_manager_operability import (
    ModelManagerHardeningFlags,
    build_model_manager_diagnostics_snapshot,
)


def _safe_env_int(name: str, default: int) -> int:
    try:
        value = get_env_int(name, default)
    except ValueError:
        return int(default)
    if value is None:
        return int(default)
    return int(value)


def _safe_env_bool(name: str, default: bool) -> bool:
    try:
        value = get_env_bool(name, default)
    except ValueError:
        return bool(default)
    if value is None:
        return bool(default)
    return bool(value)


def _safe_mode(name: str, default: str) -> str:
    raw_value = get_env_str(name, default) or default
    return raw_value.strip().lower()


def build_operator_diagnostics(*, debug: bool = False) -> dict[str, Any]:
    """Build a non-sensitive runtime diagnostics payload for operators."""
    from agent.runtime_metrics import (
        get_average_query_complexity,
        get_recent_truncation_event_count,
    )
    from agent.utils.schema_cache import (
        get_last_schema_refresh_timestamp,
        get_schema_cache_ttl_seconds,
        get_schema_snapshot_cache_size,
    )

    model_manager_flags = ModelManagerHardeningFlags.from_env()
    diagnostics = {
        "diagnostics_schema_version": 1,
        "active_database_provider": get_env_str("QUERY_TARGET_BACKEND", "postgres"),
        "retry_policy": {
            "mode": _safe_mode("AGENT_RETRY_POLICY", "adaptive"),
            "max_retries": max(0, _safe_env_int("AGENT_MAX_RETRIES", 3)),
        },
        "schema_cache_ttl_seconds": get_schema_cache_ttl_seconds(),
        "runtime_indicators": {
            "active_schema_cache_size": int(get_schema_snapshot_cache_size()),
            "last_schema_refresh_timestamp": get_last_schema_refresh_timestamp(),
            "avg_query_complexity": float(round(get_average_query_complexity(), 4)),
            "recent_truncation_event_count": int(get_recent_truncation_event_count()),
        },
        "enabled_flags": {
            "schema_binding_validation": _safe_env_bool("AGENT_SCHEMA_BINDING_VALIDATION", True),
            "schema_binding_soft_mode": _safe_env_bool("AGENT_SCHEMA_BINDING_SOFT_MODE", False),
            "column_allowlist_mode": _safe_mode("AGENT_COLUMN_ALLOWLIST_MODE", "warn"),
            "column_allowlist_from_schema_context": _safe_env_bool(
                "AGENT_COLUMN_ALLOWLIST_FROM_SCHEMA_CONTEXT", True
            ),
            "cartesian_join_mode": _safe_mode("AGENT_CARTESIAN_JOIN_MODE", "warn"),
            "capability_fallback_mode": _safe_mode("AGENT_CAPABILITY_FALLBACK_MODE", "off"),
            "provider_cap_mitigation": _safe_mode("AGENT_PROVIDER_CAP_MITIGATION", "off"),
            "decision_summary_debug": _safe_env_bool("AGENT_DEBUG_DECISION_SUMMARY", False),
            "disable_prefetch": _safe_env_bool("DISABLE_PREFETCH", False),
            "disable_schema_refresh": _safe_env_bool("DISABLE_SCHEMA_REFRESH", False),
            "disable_llm_retries": _safe_env_bool("DISABLE_LLM_RETRIES", False),
            "model_manager_strict_reload_mode": model_manager_flags.strict_reload_mode,
            "model_manager_strict_schema_mode": model_manager_flags.strict_schema_mode,
            "model_manager_schema_mismatch_warn_only": (
                model_manager_flags.schema_mismatch_warn_only
            ),
            "model_manager_calibration_strict_mode": model_manager_flags.calibration_strict_mode,
            "model_manager_drift_strict_mode": model_manager_flags.drift_strict_mode,
        },
        "model_manager": build_model_manager_diagnostics_snapshot(flags=model_manager_flags),
    }
    if debug:
        from agent.runtime_metrics import get_stage_latency_breakdown

        diagnostics["debug"] = {
            "latency_breakdown_ms": get_stage_latency_breakdown(),
        }
    return diagnostics
