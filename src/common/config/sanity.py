"""Startup-time configuration sanity checks for agent and MCP runtime flags."""

from __future__ import annotations

from typing import Iterable

from common.config.env import get_env_bool, get_env_int, get_env_str


def _normalize_mode(
    name: str,
    *,
    allowed: Iterable[str],
    default: str,
    issues: list[str],
) -> str:
    raw_value = get_env_str(name, default) or default
    normalized = raw_value.strip().lower()
    allowed_values = set(allowed)
    if normalized not in allowed_values:
        issues.append(f"{name} must be one of {sorted(allowed_values)}, got '{raw_value}'.")
        return default
    return normalized


def _read_bool(name: str, default: bool, issues: list[str]) -> bool:
    try:
        value = get_env_bool(name, default)
    except ValueError as exc:
        issues.append(str(exc))
        return default
    if value is None:
        return default
    return bool(value)


def _validate_min_int(name: str, *, minimum: int, issues: list[str]) -> None:
    raw_value = get_env_str(name, None)
    if raw_value is None:
        return
    try:
        parsed = get_env_int(name, None)
    except ValueError as exc:
        issues.append(str(exc))
        return
    if parsed is None:
        return
    if int(parsed) < int(minimum):
        issues.append(f"{name} must be >= {minimum}, got {parsed}.")


def _has_explicit_column_allowlist(raw_allowlist: str | None) -> bool:
    if not raw_allowlist:
        return False
    for token in raw_allowlist.split(","):
        normalized = token.strip().lower()
        if "." not in normalized:
            continue
        table_name, column_name = normalized.split(".", 1)
        if table_name.strip() and column_name.strip():
            return True
    return False


def validate_runtime_configuration() -> None:
    """Validate runtime configuration for incompatible or dangerous combinations.

    Raises:
        RuntimeError: when one or more invalid combinations are detected.
    """
    issues: list[str] = []

    column_allowlist_mode = _normalize_mode(
        "AGENT_COLUMN_ALLOWLIST_MODE",
        allowed={"warn", "block", "off"},
        default="warn",
        issues=issues,
    )
    _normalize_mode(
        "AGENT_CARTESIAN_JOIN_MODE",
        allowed={"warn", "block", "off"},
        default="warn",
        issues=issues,
    )
    _normalize_mode(
        "AGENT_RETRY_POLICY",
        allowed={"adaptive", "static"},
        default="adaptive",
        issues=issues,
    )
    _normalize_mode(
        "AGENT_CAPABILITY_FALLBACK_MODE",
        allowed={"off", "suggest", "apply"},
        default="off",
        issues=issues,
    )
    _normalize_mode(
        "AGENT_PROVIDER_CAP_MITIGATION",
        allowed={"off", "safe"},
        default="off",
        issues=issues,
    )

    persistence_mode = (get_env_str("AGENT_INTERACTION_PERSISTENCE_MODE", "") or "").strip().lower()
    if persistence_mode and persistence_mode not in {"best_effort", "strict"}:
        issues.append(
            "AGENT_INTERACTION_PERSISTENCE_MODE must be one of "
            "['best_effort', 'strict'] when set."
        )

    schema_binding_enabled = _read_bool("AGENT_SCHEMA_BINDING_VALIDATION", True, issues)
    schema_binding_soft_mode = _read_bool("AGENT_SCHEMA_BINDING_SOFT_MODE", False, issues)
    use_schema_columns = _read_bool("AGENT_COLUMN_ALLOWLIST_FROM_SCHEMA_CONTEXT", True, issues)
    explicit_column_allowlist = _has_explicit_column_allowlist(
        get_env_str("AGENT_COLUMN_ALLOWLIST", "")
    )

    if not schema_binding_enabled and schema_binding_soft_mode:
        issues.append(
            "AGENT_SCHEMA_BINDING_SOFT_MODE=true requires AGENT_SCHEMA_BINDING_VALIDATION=true."
        )

    if (
        column_allowlist_mode == "block"
        and not use_schema_columns
        and not explicit_column_allowlist
    ):
        issues.append(
            "AGENT_COLUMN_ALLOWLIST_MODE=block requires schema context "
            "(AGENT_COLUMN_ALLOWLIST_FROM_SCHEMA_CONTEXT=true) or explicit "
            "AGENT_COLUMN_ALLOWLIST entries."
        )

    _validate_min_int("AGENT_MAX_RETRIES", minimum=0, issues=issues)
    _validate_min_int("AGENT_RETRY_SUMMARY_MAX_EVENTS", minimum=1, issues=issues)
    _validate_min_int("AGENT_DECISION_SUMMARY_MAX_TABLES", minimum=1, issues=issues)
    _validate_min_int("SCHEMA_CACHE_TTL_SECONDS", minimum=1, issues=issues)
    _validate_min_int("DAL_SCHEMA_CACHE_TTL_SECONDS", minimum=1, issues=issues)

    if issues:
        error_lines = "\n".join(f"- {issue}" for issue in issues)
        raise RuntimeError(f"Invalid runtime configuration:\n{error_lines}")
