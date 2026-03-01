"""MCP tool: execute_sql_query - Execute read-only SQL queries."""

import asyncio
import hashlib
import inspect
import json
import logging
import time
from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any, Dict, List, Literal, Optional, Sequence

import asyncpg
from opentelemetry import trace

from agent.audit import AuditEventSource, AuditEventType, emit_audit_event
from common.config.env import get_env_bool, get_env_int, get_env_str
from common.constants.reason_codes import PayloadTruncationReason
from common.errors.error_codes import ErrorCode
from common.models.error_metadata import ErrorCategory
from common.models.tool_envelopes import ExecuteSQLQueryMetadata, ExecuteSQLQueryResponseEnvelope
from common.observability.metrics import mcp_metrics
from common.security.tenant_enforcement_policy import PolicyDecision
from common.sql.complexity import (
    ComplexityMetrics,
    analyze_sql_complexity,
    find_complexity_violation,
    get_mcp_complexity_limits,
)
from common.sql.dialect import normalize_sqlglot_dialect
from dal.capability_negotiation import (
    CapabilityNegotiationResult,
    negotiate_capability_request,
    parse_capability_fallback_policy,
)
from dal.database import Database
from dal.error_classification import emit_classified_error, extract_error_metadata
from dal.execution_resource_limits import ExecutionResourceLimits
from dal.offset_pagination import (
    OffsetPaginationTokenError,
    build_cursor_query_fingerprint,
    build_query_fingerprint,
    decode_offset_pagination_token,
    encode_offset_pagination_token,
)
from dal.pagination_cursor import (
    PAGINATION_CURSOR_SECRET_MISSING,
    CursorSigningSecretMissing,
    resolve_cursor_signing_secret,
)
from dal.postgres_sandbox import (
    SANDBOX_FAILURE_NONE,
    SANDBOX_FAILURE_REASON_ALLOWLIST,
    build_postgres_sandbox_metadata,
)
from dal.resource_containment import (
    ResourceContainmentPolicyError,
    enforce_byte_limit,
    enforce_row_limit,
    validate_resource_capabilities,
)
from dal.session_guardrails import (
    RESTRICTED_SESSION_MODE_OFF,
    SESSION_GUARDRAIL_SKIPPED,
    SessionGuardrailPolicyError,
    build_session_guardrail_metadata,
)
from dal.util.column_metadata import build_column_meta
from dal.util.row_limits import get_sync_max_rows
from dal.util.timeouts import run_with_timeout
from mcp_server.utils.provider import resolve_provider

TOOL_NAME = "execute_sql_query"
TOOL_DESCRIPTION = "Execute a validated SQL query against the target database."
logger = logging.getLogger(__name__)

_TENANT_ENFORCEMENT_OUTCOME_ALLOWLIST = {
    "APPLIED",
    "SKIPPED_NOT_REQUIRED",
    "REJECTED_UNSUPPORTED",
    "REJECTED_DISABLED",
    "REJECTED_LIMIT",
    "REJECTED_MISSING_TENANT",
    "REJECTED_TIMEOUT",
}
_SESSION_GUARDRAIL_OUTCOME_ALLOWLIST = {
    "SESSION_GUARDRAIL_APPLIED",
    "SESSION_GUARDRAIL_SKIPPED",
    "SESSION_GUARDRAIL_UNSUPPORTED_PROVIDER",
    "SESSION_GUARDRAIL_MISCONFIGURED",
}
_RESTRICTED_SESSION_MODE_ALLOWLIST = {"off", "set_local_config"}
_SANDBOX_FAILURE_REASON_ALLOWLIST = set(SANDBOX_FAILURE_REASON_ALLOWLIST)
_SESSION_RESET_OUTCOME_ALLOWLIST = {"ok", "failed"}
_SANDBOX_OUTCOME_ALLOWLIST = {"committed", "rolled_back", "rollback_failed"}
_DEFAULT_PAGE_TOKEN_MAX_LENGTH = 2048
_DEFAULT_PAGINATION_MAX_OFFSET_PAGES = 1000
_PARTIAL_REASON_ALLOWLIST = {
    PayloadTruncationReason.MAX_ROWS.value,
    PayloadTruncationReason.MAX_BYTES.value,
    PayloadTruncationReason.PROVIDER_CAP.value,
    PayloadTruncationReason.SAFETY_LIMIT.value,
    "timeout",
}
_PARTIAL_REASON_NORMALIZATION = {
    "max_rows": PayloadTruncationReason.MAX_ROWS.value,
    "max_bytes": PayloadTruncationReason.MAX_BYTES.value,
    "provider_cap": PayloadTruncationReason.PROVIDER_CAP.value,
    "safety_limit": PayloadTruncationReason.SAFETY_LIMIT.value,
    "row_limit": PayloadTruncationReason.MAX_ROWS.value,
    "byte_limit": PayloadTruncationReason.MAX_BYTES.value,
    "providercap": PayloadTruncationReason.PROVIDER_CAP.value,
    "safety": PayloadTruncationReason.SAFETY_LIMIT.value,
    "timeout": "timeout",
}
_ADAPTIVE_ROW_SIZE_FALLBACK_BYTES = 1024
_KEYSET_REJECTION_REASON_ALLOWLIST = {
    "KEYSET_ORDER_COLUMN_NOT_FOUND",
    "KEYSET_TIEBREAKER_NULLABLE",
    "KEYSET_TIEBREAKER_NOT_UNIQUE",
    "KEYSET_SCHEMA_REQUIRED",
    "KEYSET_SCHEMA_STALE",
    "KEYSET_SNAPSHOT_MISMATCH",
    "KEYSET_SNAPSHOT_REQUIRED",
    "KEYSET_TOPOLOGY_MISMATCH",
    "KEYSET_TOPOLOGY_REQUIRED",
    "KEYSET_SHARD_MISMATCH",
    "KEYSET_PARTITION_SET_CHANGED",
    "KEYSET_ISOLATION_UNSAFE",
    "KEYSET_REPLICA_LAG_UNSAFE",
    "PAGINATION_FEDERATED_ORDERING_UNSAFE",
    "PAGINATION_FEDERATED_UNSUPPORTED",
    "PAGINATION_BACKEND_SET_CHANGED",
    "PAGINATION_CURSOR_EXPIRED",
    "PAGINATION_CURSOR_ISSUED_AT_INVALID",
    "PAGINATION_CURSOR_CLOCK_SKEW",
    "PAGINATION_CURSOR_QUERY_MISMATCH",
}
_CURSOR_VALIDATION_OUTCOME_ALLOWLIST = {
    "OK",
    "EXPIRED",
    "SKEW",
    "INVALID",
    "QUERY_MISMATCH",
    "LEGACY_ACCEPTED",
    "SIGNATURE_INVALID",
    "SECRET_MISSING",
}
_CURSOR_AGE_BUCKET_ALLOWLIST = {
    "0_59",
    "60_299",
    "300_899",
    "900_3599",
    "3600_plus",
}


@dataclass(frozen=True)
class SQLASTValidationFailure:
    """Structured AST validation failure with stable classification fields."""

    message: str
    reason_code: str
    category: ErrorCategory = ErrorCategory.INVALID_REQUEST
    error_code: str = ErrorCode.VALIDATION_ERROR.value


@dataclass(frozen=True)
class KeysetSchemaLoadResult:
    """Loaded schema metadata used for keyset validation policy checks."""

    columns: Dict[str, Dict[str, Any]]
    unique_keys_by_table: Dict[str, List[List[str]]]
    loaded_tables: set[str]
    max_schema_age_seconds: float | None


class _SandboxExecutionTimeout(RuntimeError):
    """Internal control-flow exception to force sandbox rollback on timeout."""

    failure_reason = "TIMEOUT"


def _normalize_tenant_enforcement_mode(mode: str | None) -> str:
    normalized = (mode or "").strip().lower()
    if normalized == "sql_rewrite":
        return "sql_rewrite"
    if normalized == "rls_session":
        return "rls_session"
    return "none"


def _tenant_enforcement_observability_fields(
    metadata: dict[str, Any] | None,
) -> tuple[str, str, bool, str | None]:
    tenant_metadata = metadata if isinstance(metadata, dict) else {}
    mode = _normalize_tenant_enforcement_mode(tenant_metadata.get("tenant_enforcement_mode"))
    raw_outcome = str(tenant_metadata.get("tenant_rewrite_outcome") or "").strip().upper()
    outcome = (
        raw_outcome
        if raw_outcome in _TENANT_ENFORCEMENT_OUTCOME_ALLOWLIST
        else "REJECTED_UNSUPPORTED"
    )
    applied = bool(tenant_metadata.get("tenant_enforcement_applied"))
    raw_reason_code = tenant_metadata.get("tenant_rewrite_reason_code")
    reason_code = (
        raw_reason_code.strip()
        if isinstance(raw_reason_code, str) and raw_reason_code.strip()
        else None
    )
    return mode, outcome, applied, reason_code


def _record_tenant_enforcement_observability(metadata: dict[str, Any] | None) -> None:
    mode, outcome, applied, reason_code = _tenant_enforcement_observability_fields(metadata)

    span = trace.get_current_span()
    if span is not None and span.is_recording():
        span.set_attribute("tenant.enforcement.mode", mode)
        span.set_attribute("tenant.enforcement.outcome", outcome)
        span.set_attribute("tenant.enforcement.applied", applied)
        if reason_code is not None:
            span.set_attribute("tenant.enforcement.reason_code", reason_code)

    metric_attributes: dict[str, Any] = {
        "tool_name": TOOL_NAME,
        "mode": mode,
        "outcome": outcome,
        "applied": applied,
    }
    if reason_code is not None:
        metric_attributes["reason_code"] = reason_code
    mcp_metrics.add_counter(
        "mcp.tenant_enforcement.outcome_total",
        description="Count of execute_sql_query tenant enforcement outcomes",
        attributes=metric_attributes,
    )


def _session_guardrail_observability_fields(
    metadata: dict[str, Any] | None,
) -> tuple[bool, str, bool, str | None, str, str | None]:
    session_metadata = metadata if isinstance(metadata, dict) else {}
    applied = bool(session_metadata.get("session_guardrail_applied"))
    raw_outcome = str(session_metadata.get("session_guardrail_outcome") or "").strip().upper()
    outcome = (
        raw_outcome
        if raw_outcome in _SESSION_GUARDRAIL_OUTCOME_ALLOWLIST
        else SESSION_GUARDRAIL_SKIPPED
    )
    execution_role_applied = bool(session_metadata.get("execution_role_applied"))
    execution_role_name_raw = session_metadata.get("execution_role_name")
    execution_role_name = (
        execution_role_name_raw.strip()
        if isinstance(execution_role_name_raw, str) and execution_role_name_raw.strip()
        else None
    )
    restricted_mode_raw = str(session_metadata.get("restricted_session_mode") or "").strip().lower()
    restricted_mode = (
        restricted_mode_raw
        if restricted_mode_raw in _RESTRICTED_SESSION_MODE_ALLOWLIST
        else RESTRICTED_SESSION_MODE_OFF
    )
    capability_mismatch_raw = session_metadata.get("session_guardrail_capability_mismatch")
    capability_mismatch = (
        capability_mismatch_raw.strip()
        if isinstance(capability_mismatch_raw, str) and capability_mismatch_raw.strip()
        else None
    )
    return (
        applied,
        outcome,
        execution_role_applied,
        execution_role_name,
        restricted_mode,
        capability_mismatch,
    )


def _record_session_guardrail_observability(metadata: dict[str, Any] | None) -> None:
    (
        applied,
        outcome,
        execution_role_applied,
        execution_role_name,
        restricted_mode,
        capability_mismatch,
    ) = _session_guardrail_observability_fields(metadata)

    span = trace.get_current_span()
    if span is not None and span.is_recording():
        span.set_attribute("session.guardrail.applied", applied)
        span.set_attribute("session.guardrail.outcome", outcome)
        span.set_attribute("session.guardrail.execution_role_applied", execution_role_applied)
        if execution_role_name is not None:
            span.set_attribute("session.guardrail.execution_role_name", execution_role_name)
        span.set_attribute("session.guardrail.restricted_session_mode", restricted_mode)
        if capability_mismatch is not None:
            span.set_attribute("session.guardrail.capability_mismatch", capability_mismatch)

    metric_attributes: dict[str, Any] = {
        "tool_name": TOOL_NAME,
        "applied": applied,
        "outcome": outcome,
        "execution_role_applied": execution_role_applied,
        "restricted_session_mode": restricted_mode,
    }
    if execution_role_name is not None:
        metric_attributes["execution_role_name"] = execution_role_name
    if capability_mismatch is not None:
        metric_attributes["capability_mismatch"] = capability_mismatch
    mcp_metrics.add_counter(
        "mcp.session_guardrail.outcome_total",
        description="Count of execute_sql_query Postgres session guardrail outcomes",
        attributes=metric_attributes,
    )


def _extract_postgres_sandbox_metadata(source: object) -> dict[str, Any]:
    """Return bounded sandbox metadata from a connection or exception object."""
    raw_metadata = getattr(source, "postgres_sandbox_metadata", None)
    if not isinstance(raw_metadata, dict):
        return {}
    failure_reason_raw = str(raw_metadata.get("sandbox_failure_reason") or "").strip().upper()
    failure_reason = (
        failure_reason_raw
        if failure_reason_raw in _SANDBOX_FAILURE_REASON_ALLOWLIST
        else SANDBOX_FAILURE_NONE
    )
    sandbox_outcome_raw = str(raw_metadata.get("sandbox_outcome") or "").strip().lower()
    sandbox_outcome = (
        sandbox_outcome_raw if sandbox_outcome_raw in _SANDBOX_OUTCOME_ALLOWLIST else "committed"
    )
    reset_outcome_raw = str(raw_metadata.get("session_reset_outcome") or "").strip().lower()
    reset_outcome = (
        reset_outcome_raw if reset_outcome_raw in _SESSION_RESET_OUTCOME_ALLOWLIST else "failed"
    )
    return {
        "sandbox_applied": bool(raw_metadata.get("sandbox_applied")),
        "sandbox_outcome": sandbox_outcome,
        "sandbox_rollback": bool(raw_metadata.get("sandbox_rollback")),
        "sandbox_failure_reason": failure_reason,
        "session_reset_attempted": bool(raw_metadata.get("session_reset_attempted")),
        "session_reset_outcome": reset_outcome,
    }


def _bounded_db_sandbox_failure_reason(raw_reason: str) -> str | None:
    normalized = str(raw_reason or "").strip().upper()
    if normalized == "TIMEOUT":
        return "timeout"
    if normalized == "QUERY_ERROR":
        return "execution_error"
    if normalized == "ROLE_SWITCH_FAILURE":
        return "role_error"
    if normalized in {"RESET_FAILURE", "STATE_DRIFT"}:
        return "reset_error"
    if normalized == "UNKNOWN":
        return "unknown"
    return None


def _sandbox_observability_fields(
    metadata: dict[str, Any] | None,
) -> tuple[bool, str, bool, str, str | None, bool, str]:
    sandbox_metadata = metadata if isinstance(metadata, dict) else {}
    applied = bool(sandbox_metadata.get("sandbox_applied"))
    outcome_raw = str(sandbox_metadata.get("sandbox_outcome") or "").strip().lower()
    outcome = outcome_raw if outcome_raw in _SANDBOX_OUTCOME_ALLOWLIST else "committed"
    rollback = bool(sandbox_metadata.get("sandbox_rollback"))
    failure_reason_raw = str(sandbox_metadata.get("sandbox_failure_reason") or "").strip().upper()
    failure_reason = (
        failure_reason_raw
        if failure_reason_raw in _SANDBOX_FAILURE_REASON_ALLOWLIST
        else SANDBOX_FAILURE_NONE
    )
    db_failure_reason = _bounded_db_sandbox_failure_reason(failure_reason)
    reset_attempted = bool(sandbox_metadata.get("session_reset_attempted"))
    reset_outcome_raw = str(sandbox_metadata.get("session_reset_outcome") or "").strip().lower()
    reset_outcome = (
        reset_outcome_raw if reset_outcome_raw in _SESSION_RESET_OUTCOME_ALLOWLIST else "failed"
    )
    return (
        applied,
        outcome,
        rollback,
        failure_reason,
        db_failure_reason,
        reset_attempted,
        reset_outcome,
    )


def _record_sandbox_observability(metadata: dict[str, Any] | None) -> None:
    (
        applied,
        outcome,
        rollback,
        failure_reason,
        db_failure_reason,
        reset_attempted,
        reset_outcome,
    ) = _sandbox_observability_fields(metadata)
    span = trace.get_current_span()
    if span is not None and span.is_recording():
        span.set_attribute("sandbox.applied", applied)
        span.set_attribute("sandbox.rollback", rollback)
        span.set_attribute("sandbox.failure_reason", failure_reason)
        span.set_attribute("db.sandbox.applied", applied)
        span.set_attribute("db.sandbox.outcome", outcome)
        if outcome != "committed" and db_failure_reason is not None:
            span.set_attribute("db.sandbox.failure_reason", db_failure_reason)
        span.set_attribute("db.session.reset_attempted", reset_attempted)
        span.set_attribute("db.session.reset_outcome", reset_outcome)

    mcp_metrics.add_counter(
        "mcp.postgres.sandbox.outcome_total",
        description="Count of execute_sql_query Postgres sandbox execution outcomes",
        attributes={
            "tool_name": TOOL_NAME,
            "applied": applied,
            "sandbox_outcome": outcome,
            "rollback": rollback,
            "failure_reason": failure_reason,
            "db_failure_reason": db_failure_reason or "none",
            "session_reset_attempted": reset_attempted,
            "session_reset_outcome": reset_outcome,
        },
    )


def _timeout_observability_fields(metadata: dict[str, Any] | None) -> tuple[bool, bool]:
    timeout_metadata = metadata if isinstance(metadata, dict) else {}
    timeout_applied = bool(timeout_metadata.get("execution_timeout_applied"))
    timeout_triggered = bool(timeout_metadata.get("execution_timeout_triggered"))
    return timeout_applied, timeout_triggered


def _record_timeout_observability(metadata: dict[str, Any] | None) -> None:
    timeout_applied, timeout_triggered = _timeout_observability_fields(metadata)
    span = trace.get_current_span()
    if span is not None and span.is_recording():
        span.set_attribute("db.execution_timeout_applied", timeout_applied)
        span.set_attribute("db.execution_timeout_triggered", timeout_triggered)

    mcp_metrics.add_counter(
        "mcp.execution_timeout.outcome_total",
        description="Count of execute_sql_query timeout enforcement outcomes",
        attributes={
            "tool_name": TOOL_NAME,
            "execution_timeout_applied": timeout_applied,
            "execution_timeout_triggered": timeout_triggered,
        },
    )


def _bounded_keyset_rejection_reason_code(raw_reason_code: Any) -> str | None:
    reason_code = raw_reason_code.strip() if isinstance(raw_reason_code, str) else ""
    if reason_code in _KEYSET_REJECTION_REASON_ALLOWLIST:
        return reason_code
    return None


def _bounded_pagination_reject_reason_code(raw_reason_code: Any) -> str | None:
    return _bounded_keyset_rejection_reason_code(raw_reason_code)


def _normalize_execution_topology(raw_value: Any) -> str:
    normalized = str(raw_value or "").strip().lower()
    if normalized == "federated":
        return "federated"
    return "single_backend"


def _normalize_cursor_validation_outcome(raw_value: Any) -> str | None:
    if not isinstance(raw_value, str):
        return None
    normalized = raw_value.strip().upper()
    if normalized in _CURSOR_VALIDATION_OUTCOME_ALLOWLIST:
        return normalized
    return None


def _normalize_cursor_age_seconds(raw_value: Any) -> int | None:
    numeric = _normalize_non_negative_float(raw_value)
    if numeric is None:
        return None
    return min(int(numeric), 604800)


def _cursor_age_bucket(age_seconds: int | None) -> str | None:
    if age_seconds is None:
        return None
    bounded_age = _normalize_cursor_age_seconds(age_seconds)
    if bounded_age is None:
        return None
    if bounded_age < 60:
        return "0_59"
    if bounded_age < 300:
        return "60_299"
    if bounded_age < 900:
        return "300_899"
    if bounded_age < 3600:
        return "900_3599"
    return "3600_plus"


def _normalize_cursor_age_bucket(raw_value: Any) -> str | None:
    if not isinstance(raw_value, str):
        return None
    normalized = raw_value.strip()
    if normalized in _CURSOR_AGE_BUCKET_ALLOWLIST:
        return normalized
    return None


def _cursor_validation_outcome_from_reason_code(reason_code: Any) -> str | None:
    if not isinstance(reason_code, str):
        return None
    reason = reason_code.strip()
    mapping = {
        "PAGINATION_CURSOR_EXPIRED": "EXPIRED",
        "PAGINATION_CURSOR_CLOCK_SKEW": "SKEW",
        "PAGINATION_CURSOR_ISSUED_AT_INVALID": "INVALID",
        "PAGINATION_CURSOR_QUERY_MISMATCH": "QUERY_MISMATCH",
        "PAGINATION_CURSOR_SIGNATURE_INVALID": "SIGNATURE_INVALID",
        "PAGINATION_CURSOR_SECRET_MISSING": "SECRET_MISSING",
    }
    return mapping.get(reason)


def _apply_cursor_decode_metadata(
    envelope_metadata: dict[str, Any],
    decode_metadata: dict[str, Any] | None,
    *,
    fallback_reason_code: str | None = None,
) -> None:
    if not isinstance(envelope_metadata, dict):
        return
    metadata = decode_metadata if isinstance(decode_metadata, dict) else {}
    if "legacy_issued_at_accepted" in metadata:
        envelope_metadata["pagination.cursor.legacy_issued_at_accepted"] = bool(
            metadata.get("legacy_issued_at_accepted")
        )
    if "issued_at_present" in metadata:
        envelope_metadata["pagination.cursor.issued_at_present"] = bool(
            metadata.get("issued_at_present")
        )
        envelope_metadata["cursor_issued_at_present"] = bool(metadata.get("issued_at_present"))
    cursor_age_seconds = _normalize_cursor_age_seconds(metadata.get("age_s"))
    if cursor_age_seconds is not None:
        envelope_metadata["pagination.cursor.age_s"] = cursor_age_seconds
        age_bucket = _cursor_age_bucket(cursor_age_seconds)
        if age_bucket is not None:
            envelope_metadata["cursor_age_bucket"] = age_bucket
    if "expired" in metadata:
        envelope_metadata["pagination.cursor.expired"] = bool(metadata.get("expired"))
    if "skew_detected" in metadata:
        envelope_metadata["pagination.cursor.skew_detected"] = bool(metadata.get("skew_detected"))
    validation_outcome = _normalize_cursor_validation_outcome(metadata.get("validation_outcome"))
    if validation_outcome is None:
        validation_outcome = _cursor_validation_outcome_from_reason_code(fallback_reason_code)
    if validation_outcome is not None:
        envelope_metadata["pagination.cursor.validation_outcome"] = validation_outcome
        envelope_metadata["cursor_validation_outcome"] = validation_outcome
    if fallback_reason_code == "PAGINATION_CURSOR_EXPIRED":
        envelope_metadata["pagination.cursor.expired"] = True
    if fallback_reason_code == "PAGINATION_CURSOR_CLOCK_SKEW":
        envelope_metadata["pagination.cursor.skew_detected"] = True
    if fallback_reason_code == "PAGINATION_CURSOR_ISSUED_AT_INVALID":
        envelope_metadata.setdefault("cursor_issued_at_present", False)


def _normalize_keyset_context_value(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    normalized = value.strip()
    return normalized if normalized else None


def _normalize_keyset_db_role(value: Any) -> str | None:
    normalized = _normalize_keyset_context_value(value)
    if normalized is None:
        return None
    lowered = normalized.lower()
    if lowered in {"primary", "replica"}:
        return lowered
    return lowered


def _normalize_non_negative_float(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        numeric = float(value)
        return numeric if numeric >= 0 else None
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return None
        try:
            numeric = float(stripped)
        except ValueError:
            return None
        return numeric if numeric >= 0 else None
    return None


def _get_env_non_negative_float(name: str, default: float) -> float:
    raw_value = get_env_str(name, str(default))
    normalized = _normalize_non_negative_float(raw_value)
    return normalized if normalized is not None else float(default)


def _extract_keyset_cursor_context(conn: object | None) -> dict[str, str]:
    if conn is None:
        return {}
    context: dict[str, str] = {}
    snapshot_candidates = (
        "keyset_snapshot_id",
        "snapshot_id",
        "execution_snapshot_id",
    )
    transaction_candidates = (
        "keyset_transaction_id",
        "transaction_id",
        "execution_transaction_id",
    )
    db_role_candidates = (
        "keyset_db_role",
        "db_role",
        "execution_db_role",
        "read_role",
    )
    region_candidates = (
        "keyset_region",
        "region",
        "execution_region",
        "db_region",
    )
    node_id_candidates = (
        "keyset_node_id",
        "node_id",
        "execution_node_id",
        "db_node_id",
    )
    shard_id_candidates = (
        "keyset_shard_id",
        "shard_id",
        "execution_shard_id",
    )
    shard_key_hash_candidates = (
        "keyset_shard_key_hash",
        "shard_key_hash",
        "execution_shard_key_hash",
    )
    partition_signature_candidates = (
        "keyset_partition_signature",
        "partition_signature",
        "active_partition_signature",
    )
    for attr_name in snapshot_candidates:
        value = _normalize_keyset_context_value(getattr(conn, attr_name, None))
        if value is not None:
            context["snapshot_id"] = value
            break
    for attr_name in transaction_candidates:
        value = _normalize_keyset_context_value(getattr(conn, attr_name, None))
        if value is not None:
            context["transaction_id"] = value
            break
    for attr_name in db_role_candidates:
        value = _normalize_keyset_db_role(getattr(conn, attr_name, None))
        if value is not None:
            context["db_role"] = value
            break
    for attr_name in region_candidates:
        value = _normalize_keyset_context_value(getattr(conn, attr_name, None))
        if value is not None:
            context["region"] = value.lower()
            break
    for attr_name in node_id_candidates:
        value = _normalize_keyset_context_value(getattr(conn, attr_name, None))
        if value is not None:
            context["node_id"] = value
            break
    for attr_name in shard_id_candidates:
        value = _normalize_keyset_context_value(getattr(conn, attr_name, None))
        if value is not None:
            context["shard_id"] = value
            break
    for attr_name in shard_key_hash_candidates:
        value = _normalize_keyset_context_value(getattr(conn, attr_name, None))
        if value is not None:
            context["shard_key_hash"] = value
            break
    for attr_name in partition_signature_candidates:
        value = _normalize_keyset_context_value(getattr(conn, attr_name, None))
        if value is not None:
            context["partition_signature"] = value
            break
    return context


def _normalize_backend_set_member(raw_member: Any) -> tuple[str, str | None, str | None] | None:
    backend_id: str | None = None
    region: str | None = None
    role: str | None = None

    if isinstance(raw_member, str):
        backend_id = _normalize_keyset_context_value(raw_member)
    elif isinstance(raw_member, dict):
        backend_id = _normalize_keyset_context_value(
            raw_member.get("backend_id")
            or raw_member.get("backend")
            or raw_member.get("id")
            or raw_member.get("node_id")
        )
        region_raw = _normalize_keyset_context_value(raw_member.get("region"))
        region = region_raw.lower() if region_raw else None
        role = _normalize_keyset_db_role(raw_member.get("role") or raw_member.get("db_role"))
    elif isinstance(raw_member, (list, tuple)) and raw_member:
        backend_id = _normalize_keyset_context_value(raw_member[0])
        if len(raw_member) > 1:
            region_raw = _normalize_keyset_context_value(raw_member[1])
            region = region_raw.lower() if region_raw else None
        if len(raw_member) > 2:
            role = _normalize_keyset_db_role(raw_member[2])

    if backend_id is None:
        return None
    return backend_id, region, role


def _extract_backend_set_signature(
    conn: object | None,
    *,
    keyset_cursor_context: dict[str, str] | None = None,
) -> str | None:
    if conn is None:
        return None

    normalized_members: set[tuple[str, str | None, str | None]] = set()
    backend_set_candidates = (
        "keyset_backend_set",
        "backend_set",
        "active_backend_set",
        "active_backends",
        "backend_membership",
    )
    for attr_name in backend_set_candidates:
        raw_value = getattr(conn, attr_name, None)
        if isinstance(raw_value, (list, tuple, set)):
            for raw_member in raw_value:
                member = _normalize_backend_set_member(raw_member)
                if member is not None:
                    normalized_members.add(member)
        elif raw_value is not None:
            member = _normalize_backend_set_member(raw_value)
            if member is not None:
                normalized_members.add(member)
        if normalized_members:
            break

    if not normalized_members and isinstance(keyset_cursor_context, dict):
        fallback_member = _normalize_backend_set_member(
            {
                "backend_id": keyset_cursor_context.get("node_id"),
                "region": keyset_cursor_context.get("region"),
                "role": keyset_cursor_context.get("db_role"),
            }
        )
        if fallback_member is not None:
            normalized_members.add(fallback_member)

    if not normalized_members:
        return None

    canonical_members: list[dict[str, str]] = []
    for backend_id, region, role in sorted(normalized_members):
        payload = {"backend_id": backend_id}
        if region:
            payload["region"] = region
        if role:
            payload["role"] = role
        canonical_members.append(payload)

    canonical_json = json.dumps(canonical_members, separators=(",", ":"), sort_keys=True)
    return hashlib.sha256(canonical_json.encode("utf-8")).hexdigest()[:16]


async def _extract_keyset_replica_lag_seconds(conn: object | None) -> float | None:
    if conn is None:
        return None

    lag_probe = getattr(conn, "get_replica_lag_seconds", None)
    if callable(lag_probe):
        try:
            raw_lag = lag_probe()
            if inspect.isawaitable(raw_lag):
                raw_lag = await raw_lag
            lag_seconds = _normalize_non_negative_float(raw_lag)
            if lag_seconds is not None:
                return lag_seconds
        except Exception:
            pass

    for attr_name in (
        "replica_lag_seconds",
        "keyset_replica_lag_seconds",
        "db_replica_lag_seconds",
    ):
        lag_seconds = _normalize_non_negative_float(getattr(conn, attr_name, None))
        if lag_seconds is not None:
            return lag_seconds

    return None


def _normalize_isolation_level(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    normalized = " ".join(value.strip().lower().replace("_", " ").split())
    if normalized == "read committed":
        return "READ COMMITTED"
    if normalized == "repeatable read":
        return "REPEATABLE READ"
    if normalized == "serializable":
        return "SERIALIZABLE"
    if normalized == "snapshot":
        return "SNAPSHOT"
    if normalized == "read uncommitted":
        return "READ UNCOMMITTED"
    return None


def _extract_keyset_isolation_level(conn: object | None, caps: object) -> str | None:
    isolation_candidates = (
        "keyset_isolation_level",
        "isolation_level",
        "transaction_isolation",
        "db_isolation_level",
    )
    for attr_name in isolation_candidates:
        normalized = _normalize_isolation_level(getattr(conn, attr_name, None))
        if normalized is not None:
            return normalized
    capability_candidates = (
        "keyset_isolation_level",
        "isolation_level",
        "default_isolation_level",
    )
    for attr_name in capability_candidates:
        normalized = _normalize_isolation_level(getattr(caps, attr_name, None))
        if normalized is not None:
            return normalized
    return None


def _is_keyset_isolation_safe(isolation_level: str | None) -> bool:
    normalized = _normalize_isolation_level(isolation_level)
    return normalized in {"REPEATABLE READ", "SERIALIZABLE", "SNAPSHOT"}


def _record_keyset_schema_observability(metadata: dict[str, Any] | None) -> None:
    schema_metadata = metadata if isinstance(metadata, dict) else {}
    execution_topology = None
    if "pagination.execution_topology" in schema_metadata:
        execution_topology = _normalize_execution_topology(
            schema_metadata.get("pagination.execution_topology")
        )
    raw_federated_ordering_supported = schema_metadata.get(
        "pagination.federated.ordering_supported"
    )
    federated_ordering_supported = (
        bool(raw_federated_ordering_supported)
        if raw_federated_ordering_supported is not None
        else None
    )
    schema_used = bool(schema_metadata.get("pagination.keyset.schema_used"))
    schema_strict = bool(schema_metadata.get("pagination.keyset.schema_strict"))
    schema_stale = bool(schema_metadata.get("pagination.keyset.schema_stale"))
    snapshot_strict = bool(schema_metadata.get("pagination.keyset.snapshot_strict"))
    snapshot_id_present = bool(schema_metadata.get("pagination.keyset.snapshot_id_present"))
    snapshot_mismatch = bool(schema_metadata.get("pagination.keyset.snapshot_mismatch"))
    db_role_raw = schema_metadata.get("pagination.keyset.db_role")
    db_role = db_role_raw if db_role_raw in {"primary", "replica"} else "unknown"
    region_raw = schema_metadata.get("pagination.keyset.region")
    region = (
        str(region_raw).strip().lower()
        if isinstance(region_raw, str) and str(region_raw).strip()
        else "unknown"
    )
    node_id_present = bool(schema_metadata.get("pagination.keyset.node_id_present"))
    topology_mismatch = bool(schema_metadata.get("pagination.keyset.topology_mismatch"))
    topology_available = bool(schema_metadata.get("pagination.keyset.topology_available"))
    topology_strict = bool(schema_metadata.get("pagination.keyset.topology_strict"))
    shard_id_raw = schema_metadata.get("pagination.keyset.shard_id")
    shard_id = (
        str(shard_id_raw).strip()
        if isinstance(shard_id_raw, str) and str(shard_id_raw).strip()
        else "unknown"
    )
    shard_key_hash_present = bool(schema_metadata.get("pagination.keyset.shard_key_hash_present"))
    shard_mismatch = bool(schema_metadata.get("pagination.keyset.shard_mismatch"))
    shard_info_available = bool(schema_metadata.get("pagination.keyset.shard_info_available"))
    cross_shard_mode = bool(schema_metadata.get("pagination.keyset.cross_shard_mode"))
    partition_signature_raw = schema_metadata.get("pagination.keyset.partition_signature")
    partition_signature = (
        str(partition_signature_raw).strip()
        if isinstance(partition_signature_raw, str) and str(partition_signature_raw).strip()
        else "unknown"
    )
    partition_signature_available = bool(
        schema_metadata.get("pagination.keyset.partition_signature_available")
    )
    partition_set_changed = bool(schema_metadata.get("pagination.keyset.partition_set_changed"))
    backend_set_sig_present = bool(schema_metadata.get("pagination.backend_set_sig_present"))
    backend_set_mismatch = bool(schema_metadata.get("pagination.backend_set_mismatch"))
    legacy_cursor_issued_at_accepted = bool(
        schema_metadata.get("pagination.cursor.legacy_issued_at_accepted")
    )
    cursor_issued_at_present = None
    if "cursor_issued_at_present" in schema_metadata:
        cursor_issued_at_present = bool(schema_metadata.get("cursor_issued_at_present"))
    elif "pagination.cursor.issued_at_present" in schema_metadata:
        cursor_issued_at_present = bool(schema_metadata.get("pagination.cursor.issued_at_present"))
    cursor_age_seconds = _normalize_cursor_age_seconds(
        schema_metadata.get("pagination.cursor.age_s")
    )
    cursor_age_bucket = _normalize_cursor_age_bucket(schema_metadata.get("cursor_age_bucket"))
    if cursor_age_bucket is None:
        cursor_age_bucket = _cursor_age_bucket(cursor_age_seconds)
    cursor_expired = bool(schema_metadata.get("pagination.cursor.expired"))
    cursor_skew_detected = bool(schema_metadata.get("pagination.cursor.skew_detected"))
    cursor_validation_outcome = _normalize_cursor_validation_outcome(
        schema_metadata.get("pagination.cursor.validation_outcome")
    )
    if cursor_validation_outcome is None:
        cursor_validation_outcome = _normalize_cursor_validation_outcome(
            schema_metadata.get("cursor_validation_outcome")
        )
    replica_lag_seconds = _normalize_non_negative_float(
        schema_metadata.get("pagination.keyset.replica_lag_seconds")
    )
    isolation_enforced = bool(schema_metadata.get("pagination.keyset.isolation_enforced"))
    isolation_level = _normalize_isolation_level(
        schema_metadata.get("pagination.keyset.isolation_level")
    )
    if isolation_level is None:
        isolation_level = "unknown"
    rejection_reason_code = _bounded_keyset_rejection_reason_code(
        schema_metadata.get("pagination.keyset.rejection_reason_code")
    )
    pagination_reject_reason_code = (
        _bounded_pagination_reject_reason_code(schema_metadata.get("pagination.reject_reason_code"))
        or rejection_reason_code
    )

    span = trace.get_current_span()
    if span is not None and span.is_recording():
        if execution_topology is not None:
            span.set_attribute("pagination.execution_topology", execution_topology)
        if federated_ordering_supported is not None:
            span.set_attribute(
                "pagination.federated.ordering_supported", federated_ordering_supported
            )
        span.set_attribute("pagination.keyset.schema_used", schema_used)
        span.set_attribute("pagination.keyset.schema_strict", schema_strict)
        span.set_attribute("pagination.keyset.schema_stale", schema_stale)
        span.set_attribute("pagination.keyset.snapshot_strict", snapshot_strict)
        span.set_attribute("pagination.keyset.snapshot_id_present", snapshot_id_present)
        span.set_attribute("pagination.keyset.snapshot_mismatch", snapshot_mismatch)
        span.set_attribute("pagination.keyset.db_role", db_role)
        span.set_attribute("pagination.keyset.region", region)
        span.set_attribute("pagination.keyset.node_id_present", node_id_present)
        span.set_attribute("pagination.keyset.topology_mismatch", topology_mismatch)
        span.set_attribute("pagination.keyset.topology_available", topology_available)
        span.set_attribute("pagination.keyset.topology_strict", topology_strict)
        span.set_attribute("pagination.keyset.shard_id", shard_id)
        span.set_attribute("pagination.keyset.shard_key_hash_present", shard_key_hash_present)
        span.set_attribute("pagination.keyset.shard_mismatch", shard_mismatch)
        span.set_attribute("pagination.keyset.shard_info_available", shard_info_available)
        span.set_attribute("pagination.keyset.cross_shard_mode", cross_shard_mode)
        span.set_attribute("pagination.keyset.partition_signature", partition_signature)
        span.set_attribute(
            "pagination.keyset.partition_signature_available",
            partition_signature_available,
        )
        span.set_attribute("pagination.keyset.partition_set_changed", partition_set_changed)
        span.set_attribute("pagination.backend_set_sig_present", backend_set_sig_present)
        span.set_attribute("pagination.backend_set_mismatch", backend_set_mismatch)
        if "pagination.cursor.legacy_issued_at_accepted" in schema_metadata:
            span.set_attribute(
                "pagination.cursor.legacy_issued_at_accepted",
                legacy_cursor_issued_at_accepted,
            )
        if cursor_issued_at_present is not None:
            span.set_attribute("cursor_issued_at_present", cursor_issued_at_present)
        if cursor_age_seconds is not None:
            span.set_attribute("pagination.cursor.age_s", cursor_age_seconds)
        if cursor_age_bucket is not None:
            span.set_attribute("cursor_age_bucket", cursor_age_bucket)
        if "pagination.cursor.expired" in schema_metadata:
            span.set_attribute("pagination.cursor.expired", cursor_expired)
        if "pagination.cursor.skew_detected" in schema_metadata:
            span.set_attribute("pagination.cursor.skew_detected", cursor_skew_detected)
        if cursor_validation_outcome is not None:
            span.set_attribute("pagination.cursor.validation_outcome", cursor_validation_outcome)
            span.set_attribute("cursor_validation_outcome", cursor_validation_outcome)
        if replica_lag_seconds is not None:
            span.set_attribute("pagination.keyset.replica_lag_seconds", replica_lag_seconds)
        span.set_attribute("pagination.keyset.isolation_level", isolation_level)
        span.set_attribute("pagination.keyset.isolation_enforced", isolation_enforced)
        if rejection_reason_code is not None:
            span.set_attribute("pagination.keyset.rejection_reason_code", rejection_reason_code)
        if pagination_reject_reason_code is not None:
            span.set_attribute("pagination.reject_reason_code", pagination_reject_reason_code)


def _query_contains_limit_or_offset(sql: str, provider: str) -> bool:
    """Return True when SQL explicitly includes LIMIT/OFFSET clauses."""
    import sqlglot
    import sqlglot.expressions as exp

    from common.sql.comments import strip_sql_comments

    normalized_sql = strip_sql_comments(sql)
    dialect = normalize_sqlglot_dialect(provider)
    try:
        expressions = sqlglot.parse(normalized_sql, read=dialect)
    except Exception:
        # Fail closed for pagination wrapper eligibility checks.
        return True
    if not expressions:
        return True
    expression = expressions[0]
    return expression.find(exp.Limit) is not None or expression.find(exp.Offset) is not None


def _normalize_partial_reason(reason: str | None) -> str | None:
    if not isinstance(reason, str):
        return None
    normalized = _PARTIAL_REASON_NORMALIZATION.get(reason.strip().lower())
    if normalized in _PARTIAL_REASON_ALLOWLIST:
        return normalized
    return None


def _rolling_average_row_size_bytes(rows: Sequence[dict[str, Any]]) -> int | None:
    """Estimate average serialized row size using a stable rolling average."""
    if not rows:
        return None
    running_average = 0.0
    observed = 0
    for row in rows:
        try:
            row_size = len(json.dumps(row, default=str, separators=(",", ":")).encode("utf-8"))
        except Exception:
            row_size = 0
        observed += 1
        running_average += (float(row_size) - running_average) / float(observed)
    return max(1, int(round(running_average)))


def _record_result_contract_observability(
    *,
    partial: bool,
    partial_reason: str | None,
    items_returned: int,
    page_size: int | None,
    page_items_returned: int,
    next_page_token: str | None,
    bytes_returned: int,
    execution_duration_ms: int,
    pagination_mode_requested: str | None = None,
    pagination_mode_used: str | None = None,
    keyset_partial_page: bool | None = None,
    keyset_streaming_terminated: bool | None = None,
    keyset_effective_page_size: int | None = None,
    keyset_adaptive_page_size: int | None = None,
    keyset_byte_budget: int | None = None,
    keyset_cursor_emitted: bool | None = None,
    next_keyset_cursor: str | None = None,
    execution_topology: str | None = None,
    federated_ordering_supported: bool | None = None,
    backend_set_sig_present: bool | None = None,
    backend_set_mismatch: bool | None = None,
    pagination_reject_reason_code: str | None = None,
) -> None:
    span = trace.get_current_span()
    if span is None or not span.is_recording():
        return
    span.set_attribute("db.result.partial", bool(partial))
    span.set_attribute("db.result.partial_reason", partial_reason or "none")
    span.set_attribute("db.result.items_returned", int(items_returned))
    if page_size is not None:
        span.set_attribute("db.result.page_size", int(page_size))
    span.set_attribute("db.result.page_items_returned", int(page_items_returned))
    span.set_attribute("db.result.next_page_token_present", bool(next_page_token))
    if pagination_mode_used:
        span.set_attribute("db.pagination.mode", str(pagination_mode_used))
    if pagination_mode_requested:
        span.set_attribute("pagination.mode_requested", str(pagination_mode_requested))
    if pagination_mode_used:
        span.set_attribute("pagination.mode_used", str(pagination_mode_used))
    span.set_attribute("pagination.keyset.partial_page", bool(keyset_partial_page))
    span.set_attribute("pagination.keyset.streaming_terminated", bool(keyset_streaming_terminated))
    if keyset_effective_page_size is not None:
        span.set_attribute("pagination.keyset.effective_page_size", int(keyset_effective_page_size))
    if keyset_adaptive_page_size is not None:
        span.set_attribute("pagination.keyset.adaptive_page_size", int(keyset_adaptive_page_size))
    if keyset_byte_budget is not None:
        span.set_attribute("pagination.keyset.byte_budget", int(keyset_byte_budget))
    span.set_attribute("pagination.keyset.cursor_emitted", bool(keyset_cursor_emitted))
    if next_keyset_cursor:
        span.set_attribute("db.pagination.next_keyset_cursor_present", True)
    if execution_topology:
        span.set_attribute("pagination.execution_topology", str(execution_topology))
    if federated_ordering_supported is not None:
        span.set_attribute(
            "pagination.federated.ordering_supported", bool(federated_ordering_supported)
        )
    if backend_set_sig_present is not None:
        span.set_attribute("pagination.backend_set_sig_present", bool(backend_set_sig_present))
    if backend_set_mismatch is not None:
        span.set_attribute("pagination.backend_set_mismatch", bool(backend_set_mismatch))
    bounded_reject_reason_code = _bounded_pagination_reject_reason_code(
        pagination_reject_reason_code
    )
    if bounded_reject_reason_code is not None:
        span.set_attribute("pagination.reject_reason_code", bounded_reject_reason_code)
    span.set_attribute("db.result.bytes_returned", int(bytes_returned))
    span.set_attribute("db.result.execution_duration_ms", int(execution_duration_ms))


def _record_policy_decision_telemetry(attributes: dict[str, Any]) -> None:
    """Attach policy-provided telemetry attributes to the active span."""
    span = trace.get_current_span()
    if span is None or not span.is_recording():
        return
    for key, value in attributes.items():
        span.set_attribute(key, value)


def _active_provider() -> str:
    """Resolve active provider identity from capabilities with a safe fallback."""
    try:
        caps = Database.get_query_target_capabilities()
        provider_name_raw = getattr(caps, "provider_name", None)
        if not isinstance(provider_name_raw, str):
            provider_name_raw = ""
        provider_name = provider_name_raw.strip().lower()
        if provider_name and provider_name not in {"unknown", "unspecified"}:
            return resolve_provider(provider_name)
    except Exception:
        pass
    return resolve_provider(Database.get_query_target_provider())


def _build_columns_from_rows(rows: list[dict]) -> list[dict]:
    if not rows:
        return []
    first_row = rows[0]
    return [build_column_meta(key, "unknown") for key in first_row.keys()]


def _tenant_enforcement_unsupported_response(
    execution_started_at: Optional[float],
    provider: str,
    *,
    policy_decision: PolicyDecision,
    envelope_metadata: Optional[Dict[str, Any]] = None,
) -> str:
    """Return a canonical tenant-enforcement unsupported response envelope."""
    merged_metadata: Dict[str, Any] = dict(policy_decision.envelope_metadata)
    if envelope_metadata:
        merged_metadata.update(envelope_metadata)

    if policy_decision.result.outcome == "REJECTED_MISSING_TENANT":
        return _construct_error_response(
            execution_started_at,
            message=f"Tenant ID is required for {TOOL_NAME}.",
            category=ErrorCategory.INVALID_REQUEST,
            provider=provider,
            metadata={
                "sql_state": "MISSING_TENANT_ID",
                "reason_code": policy_decision.bounded_reason_code,
            },
            envelope_metadata=merged_metadata,
        )

    bounded_reason_code = policy_decision.bounded_reason_code or "tenant_enforcement_unsupported"
    message = "Tenant enforcement not supported for provider/table configuration."
    if bounded_reason_code == "tenant_rewrite_tenant_mode_unsupported":
        message = "Tenant isolation is not supported for this provider."

    return _construct_error_response(
        execution_started_at,
        message=message,
        category=ErrorCategory.TENANT_ENFORCEMENT_UNSUPPORTED,
        provider=provider,
        metadata={
            "sql_state": "TENANT_ENFORCEMENT_UNSUPPORTED",
            "error_code": ErrorCode.TENANT_ENFORCEMENT_UNSUPPORTED.value,
            "reason_code": bounded_reason_code,
        },
        envelope_metadata=merged_metadata,
    )


def _tenant_column_name() -> str:
    configured = (get_env_str("TENANT_COLUMN_NAME", "tenant_id") or "").strip()
    return configured or "tenant_id"


def _tenant_global_table_allowlist() -> set[str]:
    entries: set[str] = set()
    for env_key in ("GLOBAL_TABLE_ALLOWLIST", "TENANT_GLOBAL_TABLES"):
        raw = (get_env_str(env_key, "") or "").strip()
        if not raw:
            continue
        entries.update({entry.strip().lower() for entry in raw.split(",") if entry.strip()})
    return entries


def _extract_columns_from_table_definition(definition_payload: str) -> Optional[set[str]]:
    try:
        parsed = json.loads(definition_payload)
    except Exception:
        return None
    columns = parsed.get("columns")
    if not isinstance(columns, list):
        return None
    extracted = set()
    for entry in columns:
        if not isinstance(entry, dict):
            continue
        name = entry.get("name")
        if isinstance(name, str) and name.strip():
            extracted.add(name.strip().lower())
    return extracted or None


def _extract_keyset_column_metadata(
    definition_payload: str,
) -> Optional[tuple[dict[str, dict[str, Any]], list[list[str]], float | None]]:
    try:
        parsed = json.loads(definition_payload)
    except Exception:
        return None
    columns = parsed.get("columns")
    if not isinstance(columns, list):
        return None

    metadata: dict[str, dict[str, Any]] = {}
    for entry in columns:
        if not isinstance(entry, dict):
            continue
        name = entry.get("name")
        if not isinstance(name, str) or not name.strip():
            continue
        normalized = name.strip().lower()
        metadata[normalized] = {
            "nullable": entry.get("nullable"),
            "is_nullable": entry.get("is_nullable"),
            "is_primary_key": entry.get("is_primary_key"),
            "is_unique": entry.get("is_unique"),
            "unique": entry.get("unique"),
        }
    if not metadata:
        return None
    unique_keys = _extract_keyset_unique_keys(parsed, columns)
    schema_age_seconds = _extract_schema_age_seconds(parsed)
    return metadata, unique_keys, schema_age_seconds


def _extract_keyset_unique_keys(
    parsed_payload: dict[str, Any],
    columns: list[Any],
) -> list[list[str]]:
    unique_keys: list[list[str]] = []
    seen: set[frozenset[str]] = set()

    def _add_unique_key(raw_value: Any) -> None:
        normalized_columns = _normalize_unique_key_columns(raw_value)
        if not normalized_columns:
            return
        key_signature = frozenset(normalized_columns)
        if not key_signature or key_signature in seen:
            return
        seen.add(key_signature)
        unique_keys.append(normalized_columns)

    for entry in columns:
        if not isinstance(entry, dict):
            continue
        name = entry.get("name")
        if not isinstance(name, str) or not name.strip():
            continue
        if any(
            _is_truthy_metadata_flag(entry.get(flag))
            for flag in ("is_unique", "unique", "is_primary_key", "primary_key", "is_pk")
        ):
            _add_unique_key([name])

    for field_name in ("unique_keys", "unique_indexes", "unique_constraints"):
        raw_field = parsed_payload.get(field_name)
        if isinstance(raw_field, list):
            for entry in raw_field:
                _add_unique_key(entry)
        elif raw_field is not None:
            _add_unique_key(raw_field)

    raw_primary_key = parsed_payload.get("primary_key")
    if raw_primary_key is not None:
        _add_unique_key(raw_primary_key)

    raw_indexes = parsed_payload.get("indexes")
    if isinstance(raw_indexes, list):
        for raw_index in raw_indexes:
            if not isinstance(raw_index, dict):
                continue
            if not any(
                _is_truthy_metadata_flag(raw_index.get(flag))
                for flag in ("is_unique", "unique", "primary", "is_primary")
            ):
                continue
            _add_unique_key(raw_index.get("columns"))

    return unique_keys


def _normalize_unique_key_columns(raw_value: Any) -> list[str]:
    if isinstance(raw_value, dict):
        raw_value = raw_value.get("columns") or raw_value.get("key") or raw_value.get("column")

    if isinstance(raw_value, str):
        normalized = raw_value.strip().strip('"').lower()
        return [normalized] if normalized else []

    if not isinstance(raw_value, (list, tuple)):
        return []

    normalized_columns: list[str] = []
    for entry in raw_value:
        if isinstance(entry, dict):
            entry = entry.get("name") or entry.get("column")
        if not isinstance(entry, str):
            continue
        normalized = entry.strip().strip('"').lower()
        if normalized and normalized not in normalized_columns:
            normalized_columns.append(normalized)
    return normalized_columns


def _is_truthy_metadata_flag(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    if isinstance(value, (int, float)):
        return bool(value)
    return False


def _extract_schema_age_seconds(parsed_payload: dict[str, Any]) -> float | None:
    metadata_payload = (
        parsed_payload.get("metadata") if isinstance(parsed_payload.get("metadata"), dict) else None
    )
    candidate_payloads = [parsed_payload]
    if metadata_payload is not None:
        candidate_payloads.append(metadata_payload)

    for payload in candidate_payloads:
        for field_name in (
            "schema_age_seconds",
            "schema_snapshot_age_seconds",
            "snapshot_age_seconds",
        ):
            raw_value = payload.get(field_name)
            if raw_value is None:
                continue
            try:
                age_seconds = float(raw_value)
            except (TypeError, ValueError):
                continue
            if age_seconds >= 0:
                return age_seconds

        for field_name in ("schema_snapshot_ts", "snapshot_ts", "schema_fetched_at_ts"):
            raw_value = payload.get(field_name)
            if raw_value is None:
                continue
            try:
                snapshot_ts = float(raw_value)
            except (TypeError, ValueError):
                continue
            age_seconds = float(time.time()) - snapshot_ts
            if age_seconds >= 0:
                return age_seconds

    return None


async def _load_table_columns_for_rewrite(
    table_names: Sequence[str],
    tenant_id: int,
) -> dict[str, set[str]]:
    """Load table columns from metadata store for tenant rewrite validation."""
    if not table_names:
        return {}

    try:
        store = Database.get_metadata_store()
    except Exception:
        return {}

    table_columns: dict[str, set[str]] = {}
    for table_name in table_names:
        normalized = (table_name or "").strip().lower()
        if not normalized:
            continue
        candidates = [normalized]
        short_name = normalized.split(".")[-1]
        if short_name != normalized:
            candidates.append(short_name)

        for candidate in candidates:
            try:
                definition_payload = await store.get_table_definition(
                    candidate, tenant_id=tenant_id
                )
            except Exception:
                continue
            columns = _extract_columns_from_table_definition(definition_payload)
            if not columns:
                continue
            table_columns[normalized] = columns
            table_columns[candidate] = columns
            break

    return table_columns


async def _load_keyset_column_metadata(
    table_names: Sequence[str], tenant_id: int | None
) -> KeysetSchemaLoadResult:
    """Best-effort metadata loader for keyset tie-breaker validation."""
    if not table_names:
        return KeysetSchemaLoadResult(
            columns={},
            unique_keys_by_table={},
            loaded_tables=set(),
            max_schema_age_seconds=None,
        )

    try:
        store = Database.get_metadata_store()
    except Exception:
        return KeysetSchemaLoadResult(
            columns={},
            unique_keys_by_table={},
            loaded_tables=set(),
            max_schema_age_seconds=None,
        )

    loaded: dict[str, dict[str, Any]] = {}
    loaded_unique_keys: dict[str, list[list[str]]] = {}
    loaded_tables: set[str] = set()
    max_schema_age_seconds: float | None = None
    single_table = len(table_names) == 1
    for table_name in table_names:
        normalized = (table_name or "").strip().lower()
        if not normalized:
            continue

        candidates = [normalized]
        short_name = normalized.split(".")[-1]
        if short_name != normalized:
            candidates.append(short_name)

        table_metadata: dict[str, dict[str, Any]] | None = None
        table_unique_keys: list[list[str]] = []
        table_schema_age_seconds: float | None = None
        resolved_table_name = short_name
        for candidate in candidates:
            try:
                definition_payload = await store.get_table_definition(
                    candidate, tenant_id=tenant_id
                )
            except Exception:
                continue
            extracted = _extract_keyset_column_metadata(definition_payload)
            if extracted:
                table_metadata, table_unique_keys, table_schema_age_seconds = extracted
                resolved_table_name = candidate.split(".")[-1]
                break

        if not table_metadata:
            continue

        for column_name, metadata in table_metadata.items():
            loaded[f"{resolved_table_name}.{column_name}"] = metadata
            if single_table:
                loaded[column_name] = metadata

        if table_unique_keys:
            loaded_unique_keys[resolved_table_name] = table_unique_keys
            loaded_unique_keys[normalized] = table_unique_keys
            if short_name:
                loaded_unique_keys[short_name] = table_unique_keys

        loaded_tables.add(normalized)
        if short_name:
            loaded_tables.add(short_name)
        loaded_tables.add(resolved_table_name)

        if table_schema_age_seconds is not None:
            if max_schema_age_seconds is None:
                max_schema_age_seconds = float(table_schema_age_seconds)
            else:
                max_schema_age_seconds = max(
                    max_schema_age_seconds, float(table_schema_age_seconds)
                )

    return KeysetSchemaLoadResult(
        columns=loaded,
        unique_keys_by_table=loaded_unique_keys,
        loaded_tables=loaded_tables,
        max_schema_age_seconds=max_schema_age_seconds,
    )


def _keyset_tiebreaker_allowlist() -> set[str]:
    raw = (get_env_str("MCP_KEYSET_TIEBREAKER_ALLOWLIST", "") or "").strip()
    if not raw:
        return set()
    return {entry.strip().lower() for entry in raw.split(",") if entry.strip()}


def _resolve_row_limit(conn: object) -> int:
    def _coerce_limit(value: object) -> int:
        if isinstance(value, bool):
            return 0
        if isinstance(value, int):
            return max(0, value)
        if isinstance(value, float):
            return max(0, int(value))
        if isinstance(value, str):
            stripped = value.strip()
            if not stripped:
                return 0
            try:
                return max(0, int(stripped))
            except ValueError:
                return 0
        return 0

    max_rows = _coerce_limit(getattr(conn, "max_rows", None))
    if max_rows <= 0:
        max_rows = _coerce_limit(getattr(conn, "_max_rows", None))
    if max_rows <= 0:
        max_rows = _coerce_limit(get_sync_max_rows())
    return max_rows


async def _cancel_best_effort(conn: object) -> None:
    cancel_fn = getattr(conn, "cancel", None)
    job_id = getattr(conn, "last_job_id", None) or getattr(conn, "job_id", None)
    if callable(cancel_fn):
        try:
            if job_id:
                await cancel_fn(job_id)
            else:
                await cancel_fn()
        except Exception:
            pass
    executor = getattr(conn, "executor", None)
    if executor is None:
        return
    cancel_executor = getattr(executor, "cancel", None)
    if callable(cancel_executor) and job_id:
        try:
            await cancel_executor(job_id)
        except Exception:
            pass


def _construct_error_response(
    execution_started_at: Optional[float],
    message: str,
    category: ErrorCategory = ErrorCategory.UNKNOWN,
    metadata: Optional[Dict[str, Any]] = None,
    envelope_metadata: Optional[Dict[str, Any]] = None,
    provider: str | None = None,
    is_retryable: bool = False,
    retry_after_seconds: Optional[float] = None,
) -> str:
    """Construct a standardized error response."""
    from mcp_server.utils.errors import build_error_metadata

    resolved_provider = resolve_provider(provider)
    envelope_meta = dict(envelope_metadata or {})

    # Envelope Mode (Legacy mode removed as per hardening requirements)
    meta_dict = (metadata or {}).copy()
    # Remove keys that are passed explicitly to avoid multiple values error
    for key in [
        "message",
        "category",
        "provider",
        "is_retryable",
        "retry_after_seconds",
        "error_code",
    ]:
        meta_dict.pop(key, None)

    error_meta = build_error_metadata(
        message=message,
        category=category,
        provider=resolved_provider,
        retryable=is_retryable,
        retry_after_seconds=retry_after_seconds,
        code=meta_dict.get("sql_state"),
        error_code=(metadata or {}).get("error_code"),
        hint=meta_dict.get("hint"),
    )

    details_safe = (
        error_meta.details_safe.copy() if isinstance(error_meta.details_safe, dict) else {}
    )
    if meta_dict:
        details_safe.update(
            {key: value for key, value in meta_dict.items() if key not in {"sql_state", "hint"}}
        )

    updates = {}
    if details_safe:
        updates["details_safe"] = details_safe

    # Sync reason_code to top level if present
    # Some older tests expect it in details_safe specifically, so we keep it there too.
    if "reason_code" in meta_dict:
        updates["reason_code"] = meta_dict["reason_code"]

    if updates:
        error_meta = error_meta.model_copy(update=updates)

    pagination_rejection_reason = _bounded_pagination_reject_reason_code(
        meta_dict.get("reason_code")
    )
    if pagination_rejection_reason is not None:
        envelope_meta["pagination.reject_reason_code"] = pagination_rejection_reason
        if envelope_meta.get("pagination_mode_used") == "keyset":
            envelope_meta["pagination.keyset.rejection_reason_code"] = pagination_rejection_reason

    _record_tenant_enforcement_observability(envelope_meta)
    _record_session_guardrail_observability(envelope_meta)
    _record_sandbox_observability(envelope_meta)
    _record_timeout_observability(envelope_meta)
    _record_keyset_schema_observability(envelope_meta)

    envelope = ExecuteSQLQueryResponseEnvelope(
        rows=[],
        metadata=ExecuteSQLQueryMetadata(
            rows_returned=0,
            is_truncated=False,
            execution_duration_ms=max(
                0, int((time.monotonic() - (execution_started_at or time.monotonic())) * 1000)
            ),
            provider=resolved_provider,
            row_limit=envelope_meta.get("row_limit") or 0,
            **envelope_meta,
        ),
        error=error_meta,
    )
    return envelope.model_dump_json(exclude_none=True, by_alias=True)


def _record_complexity_attributes(
    metrics: ComplexityMetrics, *, limit_exceeded: bool = False
) -> None:
    span = trace.get_current_span()
    if span is None or not span.is_recording():
        return
    span.set_attribute("sql.complexity.score", int(metrics.score))
    span.set_attribute("sql.complexity.joins", int(metrics.joins))
    span.set_attribute("sql.complexity.ctes", int(metrics.ctes))
    span.set_attribute("sql.complexity.subquery_depth", int(metrics.subquery_depth))
    span.set_attribute("sql.complexity.cartesian_join_detected", bool(metrics.has_cartesian))
    if metrics.projection_count is not None:
        span.set_attribute("sql.complexity.projection_count", int(metrics.projection_count))
    if limit_exceeded:
        span.set_attribute("sql.complexity.limit_exceeded", True)


def _complexity_violation_message(
    limit_name: str,
    measured: int | bool,
    limit: int | bool,
) -> str:
    if limit_name == "cartesian_join":
        return "SQL query rejected by complexity guard: cartesian joins are not allowed."
    if limit_name == "complexity_score":
        return (
            "SQL query rejected by complexity guard: complexity score exceeds the configured limit."
        )
    if limit_name == "projection_count":
        return "SQL query rejected by complexity guard: projected column count exceeds the limit."
    if limit_name == "subquery_depth":
        return (
            "SQL query rejected by complexity guard: subquery nesting depth exceeds "
            "the allowed limit."
        )
    if limit_name == "ctes":
        return "SQL query rejected by complexity guard: CTE count exceeds the allowed limit."
    if limit_name == "joins":
        return "SQL query rejected by complexity guard: join count exceeds the allowed limit."
    return "SQL query rejected by complexity guard: configured complexity limits exceeded."


def _validate_sql_complexity(
    sql: str, provider: str
) -> tuple[Optional[str], Optional[dict[str, Any]]]:
    from common.sql.comments import strip_sql_comments

    stripped_sql = strip_sql_comments(sql)
    dialect = normalize_sqlglot_dialect(provider)
    try:
        metrics = analyze_sql_complexity(stripped_sql, dialect=dialect)
    except Exception:
        return None, None

    _record_complexity_attributes(metrics)
    limits = get_mcp_complexity_limits()
    violation = find_complexity_violation(metrics, limits)
    complexity_meta = {
        "complexity_score": metrics.score,
        "joins": metrics.joins,
        "ctes": metrics.ctes,
        "subquery_depth": metrics.subquery_depth,
        "cartesian_join_detected": metrics.has_cartesian,
    }
    if metrics.projection_count is not None:
        complexity_meta["projection_count"] = metrics.projection_count

    if violation is None:
        return None, complexity_meta

    _record_complexity_attributes(metrics, limit_exceeded=True)
    complexity_meta.update(
        {
            "complexity_limit_name": violation.limit_name,
            "complexity_limit_value": violation.limit,
            "complexity_limit_measured": violation.measured,
        }
    )
    return (
        _complexity_violation_message(
            limit_name=violation.limit_name,
            measured=violation.measured,
            limit=violation.limit,
        ),
        complexity_meta,
    )


def _validate_sql_ast_failure(sql: str, provider: str) -> Optional[SQLASTValidationFailure]:
    """Return structured SQL AST validation failure, or None when valid."""
    import sqlglot

    from common.sql.comments import strip_sql_comments

    # Map Text2SQL provider names to sqlglot dialects
    dialect = normalize_sqlglot_dialect(provider)
    stripped_sql = strip_sql_comments(sql)

    try:
        expressions = sqlglot.parse(stripped_sql, read=dialect)
        if not expressions:
            return SQLASTValidationFailure(
                message="Empty or invalid SQL query.",
                reason_code="invalid_sql_ast",
            )

        if len(expressions) > 1:
            return SQLASTValidationFailure(
                message="Multi-statement queries are forbidden.",
                reason_code="multi_statement_forbidden",
            )

        expression = expressions[0]
        if expression is None:
            return SQLASTValidationFailure(
                message="Failed to parse SQL query.",
                reason_code="invalid_sql_ast",
            )

        # Use centralized policy
        from common.policy.sql_policy import (
            ALLOWED_STATEMENT_TYPES,
            classify_blocked_table_reference,
            classify_sql_policy_violation,
        )

        policy_violation = classify_sql_policy_violation(expression)
        if policy_violation is not None:
            if policy_violation.reason_code == "blocked_function":
                function_name = (policy_violation.function or "UNKNOWN").upper()
                return SQLASTValidationFailure(
                    message=f"Forbidden function: {function_name} is not allowed.",
                    reason_code=policy_violation.reason_code,
                    category=policy_violation.category,
                    error_code=policy_violation.error_code,
                )

            if policy_violation.reason_code == "blocked_statement":
                statement_name = (policy_violation.statement or "UNKNOWN").upper()
                return SQLASTValidationFailure(
                    message=f"Forbidden statement: {statement_name} is not allowed.",
                    reason_code=policy_violation.reason_code,
                    category=policy_violation.category,
                    error_code=policy_violation.error_code,
                )

            if policy_violation.reason_code == "statement_type_not_allowed":
                allowed_list = ", ".join(sorted([t.upper() for t in ALLOWED_STATEMENT_TYPES]))
                statement_name = (policy_violation.statement or expression.key).upper()
                return SQLASTValidationFailure(
                    message=(
                        f"Forbidden statement type: {statement_name}. "
                        f"Only {allowed_list} are allowed."
                    ),
                    reason_code=policy_violation.reason_code,
                    category=policy_violation.category,
                    error_code=policy_violation.error_code,
                )

            if policy_violation.reason_code == "readonly_violation":
                form_name = (policy_violation.statement or "UNKNOWN").upper()
                return SQLASTValidationFailure(
                    message=f"Forbidden read-only violation: '{form_name}' is not allowed.",
                    reason_code=policy_violation.reason_code,
                    category=policy_violation.category,
                    error_code=policy_violation.error_code,
                )

        # Block restricted/system tables and schemas for direct MCP invocations.
        import sqlglot.expressions as exp

        for table in expression.find_all(exp.Table):
            table_name = table.name.lower() if table.name else ""
            schema_name = table.db.lower() if table.db else ""
            blocked_reason = classify_blocked_table_reference(
                table_name=table_name,
                schema_name=schema_name,
            )
            if blocked_reason is None:
                continue
            full_name = f"{schema_name}.{table_name}" if schema_name else table_name
            if blocked_reason == "restricted_table":
                return SQLASTValidationFailure(
                    message=f"Forbidden table: {full_name} is not allowed.",
                    reason_code=blocked_reason,
                )
            return SQLASTValidationFailure(
                message=f"Forbidden schema/table reference: {full_name} is not allowed.",
                reason_code=blocked_reason,
            )

    except sqlglot.errors.ParseError as e:
        return SQLASTValidationFailure(
            message=f"SQL Syntax Error: {e}",
            reason_code="invalid_sql_syntax",
        )
    except Exception:
        return SQLASTValidationFailure(
            message="SQL Validation Error.",
            reason_code="sql_validation_error",
        )

    return None


def _validate_sql_ast(sql: str, provider: str) -> Optional[str]:
    """Validate SQL AST using sqlglot to ensure single-statement SELECT only."""
    failure = _validate_sql_ast_failure(sql, provider)
    return failure.message if failure is not None else None


def _validate_params(params: Optional[list]) -> Optional[str]:
    """Validate parameters to ensure they are a flat list of scalars."""
    if params is None:
        return None

    if not isinstance(params, (list, tuple)):
        # We accept list or tuple as "list-like", but the type hint says list.
        # Strict requirement says "must be a list".
        # Let's stick to list check if the requirement is strict.
        # However, `handler` signature says `params: Optional[list]`.
        # I will check for list.
        return "Parameters must be a list."

    for i, param in enumerate(params):
        if param is None:
            continue
        if not isinstance(param, (str, int, float, bool)):
            return (
                f"Parameter at index {i} has unsupported type: {type(param).__name__}. "
                "Only scalar values (str, int, float, bool, None) are allowed."
            )

    return None


def _resolve_effective_timeout_seconds(
    timeout_seconds: Optional[float],
    resource_limits: ExecutionResourceLimits,
) -> tuple[Optional[float], bool]:
    """Resolve effective timeout with fail-safe enforcement from resource settings."""
    resolved_timeout: Optional[float]
    try:
        resolved_timeout = float(timeout_seconds) if timeout_seconds is not None else None
    except (TypeError, ValueError):
        resolved_timeout = None

    if resolved_timeout is not None and resolved_timeout <= 0:
        resolved_timeout = None

    if resource_limits.enforce_timeout:
        resource_timeout = max(0.001, float(resource_limits.max_execution_ms) / 1000.0)
        if resolved_timeout is None:
            resolved_timeout = resource_timeout
        else:
            resolved_timeout = min(resolved_timeout, resource_timeout)

    timeout_applied = bool(resolved_timeout and resolved_timeout > 0)
    return resolved_timeout, timeout_applied


async def handler(
    sql_query: str,
    tenant_id: Optional[int],
    params: Optional[List[Any]] = None,
    include_columns: bool = True,
    timeout_seconds: Optional[float] = None,
    page_token: Optional[str] = None,
    page_size: Optional[int] = None,
    pagination_mode: Literal["offset", "keyset"] = "offset",
    keyset_cursor: Optional[str] = None,
    keyset_order_by: Optional[List[str]] = None,
    streaming: bool = False,
) -> str:
    """Execute a validated SQL query against the target database.

    Authorization:
        Requires 'SQL_ADMIN_ROLE' for execution.

    Data Access:
        Read-only access to the scoped tenant database. Mutations (INSERT, UPDATE, DELETE, etc.)
        are strictly blocked at Agent, MCP, and Database driver levels.

    Failure Modes:
        - Forbidden statement type: If mutation is detected.
        - Unauthorized: If the required role is missing.
        - Timeout: If execution exceeds the allotted time.
        - Capacity detection: If query triggers row/resource caps.
    """
    provider = _active_provider()
    import time

    execution_started_at = time.monotonic()
    keyset_order_keys = []
    keyset_values = []
    keyset_table_names: List[str] = []
    keyset_column_metadata: Dict[str, Dict[str, Any]] = {}
    keyset_unique_keys_by_table: Dict[str, List[List[str]]] = {}
    keyset_schema_info = None
    keyset_order_signature: List[str] = []
    keyset_rewritten_select = None
    streaming_terminated_early = False
    keyset_schema_strict = bool(get_env_bool("KEYSET_SCHEMA_STRICT", False))
    keyset_schema_ttl_seconds = max(0, int(get_env_int("KEYSET_SCHEMA_TTL_SECONDS", 300) or 0))
    keyset_snapshot_strict = bool(get_env_bool("KEYSET_STRICT_SNAPSHOT", False))
    keyset_topology_strict = bool(get_env_bool("KEYSET_STRICT_TOPOLOGY", False))
    keyset_max_replica_lag_seconds = _get_env_non_negative_float(
        "KEYSET_MAX_REPLICA_LAG_SECONDS", 0.0
    )
    keyset_allow_weaker_isolation = bool(get_env_bool("KEYSET_ALLOW_WEAKER_ISOLATION", False))
    keyset_allow_cross_shard = bool(get_env_bool("KEYSET_ALLOW_CROSS_SHARD", False))
    try:
        resource_limits = ExecutionResourceLimits.from_env()
    except ValueError:
        return _construct_error_response(
            execution_started_at,
            message="Execution resource limits are misconfigured.",
            category=ErrorCategory.INTERNAL,
            provider=provider,
            metadata={"reason_code": "execution_resource_limits_misconfigured"},
        )
    effective_timeout_seconds, execution_timeout_applied = _resolve_effective_timeout_seconds(
        timeout_seconds, resource_limits
    )
    session_guardrail_metadata = build_session_guardrail_metadata(
        applied=False,
        outcome=SESSION_GUARDRAIL_SKIPPED,
        execution_role_applied=False,
        execution_role_name=None,
        restricted_session_mode=RESTRICTED_SESSION_MODE_OFF,
    )
    sandbox_metadata = build_postgres_sandbox_metadata(
        applied=False,
        rollback=False,
        failure_reason=SANDBOX_FAILURE_NONE,
    )
    # Initialize metadata for consistent error responses
    tenant_enforcement_metadata: Dict[str, Any] = {
        "pagination_mode_requested": pagination_mode,
        "pagination_mode_used": pagination_mode,
        "next_keyset_cursor": None,
        "execution_timeout_applied": execution_timeout_applied,
        "execution_timeout_triggered": False,
        "resource_capability_mismatch": None,
    }
    if pagination_mode == "keyset":
        tenant_enforcement_metadata["pagination.keyset.partial_page"] = False
        tenant_enforcement_metadata["pagination.keyset.cursor_emitted"] = False
        tenant_enforcement_metadata["pagination.backend_set_sig_present"] = False
        tenant_enforcement_metadata["pagination.backend_set_mismatch"] = False
        tenant_enforcement_metadata["pagination.keyset.byte_budget"] = max(
            0, int(resource_limits.max_bytes)
        )
        tenant_enforcement_metadata["pagination.keyset.schema_used"] = False
        tenant_enforcement_metadata["pagination.keyset.schema_strict"] = keyset_schema_strict
        tenant_enforcement_metadata["pagination.keyset.schema_stale"] = False
        tenant_enforcement_metadata["pagination.keyset.snapshot_strict"] = keyset_snapshot_strict
        tenant_enforcement_metadata["pagination.keyset.snapshot_id_present"] = False
        tenant_enforcement_metadata["pagination.keyset.snapshot_mismatch"] = False
        tenant_enforcement_metadata["pagination.keyset.db_role"] = "unknown"
        tenant_enforcement_metadata["pagination.keyset.region"] = "unknown"
        tenant_enforcement_metadata["pagination.keyset.node_id_present"] = False
        tenant_enforcement_metadata["pagination.keyset.topology_mismatch"] = False
        tenant_enforcement_metadata["pagination.keyset.topology_available"] = False
        tenant_enforcement_metadata["pagination.keyset.topology_strict"] = keyset_topology_strict
        tenant_enforcement_metadata["pagination.keyset.shard_id"] = "unknown"
        tenant_enforcement_metadata["pagination.keyset.shard_key_hash_present"] = False
        tenant_enforcement_metadata["pagination.keyset.shard_mismatch"] = False
        tenant_enforcement_metadata["pagination.keyset.shard_info_available"] = False
        tenant_enforcement_metadata["pagination.keyset.cross_shard_mode"] = keyset_allow_cross_shard
        tenant_enforcement_metadata["pagination.keyset.partition_signature"] = "unknown"
        tenant_enforcement_metadata["pagination.keyset.partition_signature_available"] = False
        tenant_enforcement_metadata["pagination.keyset.partition_set_changed"] = False
        tenant_enforcement_metadata["pagination.keyset.replica_lag_seconds"] = None
        tenant_enforcement_metadata["pagination.keyset.isolation_level"] = "unknown"
        tenant_enforcement_metadata["pagination.keyset.isolation_enforced"] = (
            not keyset_allow_weaker_isolation
        )
        tenant_enforcement_metadata["pagination.keyset.rejection_reason_code"] = None
        if streaming:
            tenant_enforcement_metadata["pagination.keyset.streaming_terminated"] = False
    tenant_enforcement_metadata.update(session_guardrail_metadata)
    tenant_enforcement_metadata.update(sandbox_metadata)

    from mcp_server.utils.auth import validate_role

    if validate_role("SQL_ADMIN_ROLE", TOOL_NAME):
        return _construct_error_response(
            execution_started_at,
            message="Unauthorized: SQL_ADMIN_ROLE required.",
            category=ErrorCategory.AUTHENTICATION_FAILED,
            provider=provider,
            metadata={"reason_code": "unauthorized"},
            envelope_metadata=tenant_enforcement_metadata,
        )

    # 5. Execute Query
    try:
        caps = Database.get_query_target_capabilities()
    except Exception:
        caps = SimpleNamespace(
            provider_name=provider,
            tenant_enforcement_mode="rls_session",
            supports_column_metadata=True,
            supports_cancel=True,
            supports_pagination=True,
            execution_model="sync",
            execution_topology="single_backend",
            supports_federated_deterministic_ordering=False,
        )
    tenant_mode_raw = getattr(caps, "tenant_enforcement_mode", None)
    tenant_enforcement_mode = (
        tenant_mode_raw.strip().lower()
        if isinstance(tenant_mode_raw, str) and tenant_mode_raw.strip()
        else "rls_session"
    )

    from common.security.tenant_enforcement_policy import TenantEnforcementPolicy
    from common.sql.tenant_sql_rewriter import load_tenant_rewrite_settings

    rewrite_settings = load_tenant_rewrite_settings()
    policy = TenantEnforcementPolicy(
        provider=provider,
        mode=tenant_enforcement_mode,
        strict=rewrite_settings.strict_mode,
        max_targets=rewrite_settings.max_targets,
        max_params=rewrite_settings.max_params,
        max_ast_nodes=rewrite_settings.max_ast_nodes,
        hard_timeout_ms=rewrite_settings.hard_timeout_ms,
        warn_ms=rewrite_settings.warn_ms,
        rewrite_enabled=rewrite_settings.enabled,
    )
    policy_decision = policy.default_decision(sql=sql_query, params=params)
    tenant_enforcement_metadata.update(policy_decision.envelope_metadata)

    try:
        validate_resource_capabilities(
            provider=provider,
            enforce_row_limit=resource_limits.enforce_row_limit,
            enforce_byte_limit=resource_limits.enforce_byte_limit,
            enforce_timeout=resource_limits.enforce_timeout,
            supports_row_cap=bool(getattr(caps, "supports_row_cap", True)),
            supports_byte_cap=bool(getattr(caps, "supports_byte_cap", True)),
            supports_timeout=bool(getattr(caps, "supports_timeout", True)),
        )
    except ResourceContainmentPolicyError as e:
        tenant_enforcement_metadata["resource_capability_mismatch"] = e.reason_code
        return _construct_error_response(
            execution_started_at,
            message=str(e),
            category=ErrorCategory.UNSUPPORTED_CAPABILITY,
            provider=provider,
            metadata={
                "reason_code": e.reason_code,
                "required_capability": e.required_capability,
            },
            envelope_metadata=tenant_enforcement_metadata,
        )

    if tenant_id is not None and not policy_decision.should_execute:
        _record_policy_decision_telemetry(policy_decision.telemetry_attributes)
        return _tenant_enforcement_unsupported_response(
            execution_started_at,
            provider,
            policy_decision=policy_decision,
            envelope_metadata=tenant_enforcement_metadata,
        )

    effective_sql_query = policy_decision.sql_to_execute
    effective_params = list(policy_decision.params_to_bind)

    # 1. SQL Length Check
    max_sql_len = get_env_int("MCP_MAX_SQL_LENGTH", 100 * 1024)
    if len(sql_query) > max_sql_len:
        return _construct_error_response(
            execution_started_at,
            message=f"SQL query exceeds maximum length of {max_sql_len} bytes.",
            category=ErrorCategory.INVALID_REQUEST,
            provider=provider,
            envelope_metadata=tenant_enforcement_metadata,
        )

    # 1.5 Pagination Mode and Bounded Fields Validation
    if page_token and keyset_cursor:
        return _construct_error_response(
            execution_started_at,
            message="Pagination token mode mismatch: provide only one pagination token type.",
            category=ErrorCategory.INVALID_REQUEST,
            provider=provider,
            metadata={"reason_code": "PAGINATION_MODE_TOKEN_MISMATCH"},
            envelope_metadata=tenant_enforcement_metadata,
        )
    if pagination_mode == "keyset" and page_token:
        return _construct_error_response(
            execution_started_at,
            message="Keyset pagination mode does not accept page_token.",
            category=ErrorCategory.INVALID_REQUEST,
            provider=provider,
            metadata={"reason_code": "PAGINATION_MODE_TOKEN_MISMATCH"},
            envelope_metadata=tenant_enforcement_metadata,
        )
    if pagination_mode == "offset" and keyset_cursor:
        return _construct_error_response(
            execution_started_at,
            message="Offset pagination mode does not accept keyset_cursor.",
            category=ErrorCategory.INVALID_REQUEST,
            provider=provider,
            metadata={"reason_code": "PAGINATION_MODE_TOKEN_MISMATCH"},
            envelope_metadata=tenant_enforcement_metadata,
        )

    if pagination_mode == "keyset":
        supports_keyset_fallback = bool(getattr(caps, "supports_pagination", False)) or (
            bool(getattr(caps, "supports_offset_pagination_wrapper", False))
            and bool(getattr(caps, "supports_query_wrapping_subselect", False))
        )
        supports_keyset = bool(getattr(caps, "supports_keyset", supports_keyset_fallback))
        supports_keyset_with_containment_fallback = (
            supports_keyset
            and bool(getattr(caps, "supports_row_cap", True))
            and bool(getattr(caps, "supports_byte_cap", True))
            and bool(getattr(caps, "supports_timeout", True))
        )
        supports_keyset_with_containment = bool(
            getattr(
                caps,
                "supports_keyset_with_containment",
                supports_keyset_with_containment_fallback,
            )
        )

        if not supports_keyset:
            return _construct_error_response(
                execution_started_at,
                "Keyset pagination is not supported for this provider.",
                category=ErrorCategory.INVALID_REQUEST,
                provider=provider,
                metadata={
                    "reason_code": "execution_pagination_unsupported_provider",
                    "required_capability": "keyset_pagination",
                },
                envelope_metadata=tenant_enforcement_metadata,
            )
        if not supports_keyset_with_containment:
            return _construct_error_response(
                execution_started_at,
                "Keyset pagination with resource containment is not supported for this provider.",
                category=ErrorCategory.INVALID_REQUEST,
                provider=provider,
                metadata={
                    "reason_code": "execution_pagination_unsupported_provider",
                    "required_capability": "keyset_with_containment",
                },
                envelope_metadata=tenant_enforcement_metadata,
            )

        from dal.keyset_pagination import (
            KEYSET_ORDER_COLUMN_NOT_FOUND,
            KEYSET_REQUIRES_STABLE_TIEBREAKER,
            KEYSET_SCHEMA_REQUIRED,
            KEYSET_SCHEMA_STALE,
            KEYSET_TIEBREAKER_NOT_UNIQUE,
            KEYSET_TIEBREAKER_NULLABLE,
            StaticSchemaInfoProvider,
            build_keyset_order_signature,
            extract_keyset_order_keys,
            extract_keyset_table_names,
            validate_stable_tiebreaker,
        )

        try:
            keyset_order_keys = extract_keyset_order_keys(sql_query, provider=provider)
            keyset_table_names = extract_keyset_table_names(sql_query, provider=provider)
        except ValueError as e:
            return _construct_error_response(
                execution_started_at,
                message=str(e),
                category=ErrorCategory.INVALID_REQUEST,
                provider=provider,
                metadata={"reason_code": "execution_pagination_keyset_invalid_sql"},
                envelope_metadata=tenant_enforcement_metadata,
            )

        if not keyset_order_keys:
            return _construct_error_response(
                execution_started_at,
                message=(
                    "Keyset pagination requires an ORDER BY clause with "
                    "deterministic expressions."
                ),
                category=ErrorCategory.INVALID_REQUEST,
                provider=provider,
                metadata={"reason_code": "execution_pagination_keyset_order_by_required"},
                envelope_metadata=tenant_enforcement_metadata,
            )

        keyset_schema_load = await _load_keyset_column_metadata(keyset_table_names, tenant_id)
        keyset_column_metadata = keyset_schema_load.columns
        keyset_unique_keys_by_table = keyset_schema_load.unique_keys_by_table

        if keyset_schema_strict:
            normalized_tables = {
                (table_name or "").strip().lower().split(".")[-1]
                for table_name in keyset_table_names
                if isinstance(table_name, str) and table_name.strip()
            }
            has_full_schema_coverage = bool(normalized_tables) and all(
                table_name in keyset_schema_load.loaded_tables for table_name in normalized_tables
            )
            if not has_full_schema_coverage:
                tenant_enforcement_metadata["pagination.keyset.rejection_reason_code"] = (
                    KEYSET_SCHEMA_REQUIRED
                )
                return _construct_error_response(
                    execution_started_at,
                    message=(
                        "Keyset pagination requires schema metadata for referenced ORDER BY tables "
                        "when strict schema mode is enabled."
                    ),
                    category=ErrorCategory.INVALID_REQUEST,
                    provider=provider,
                    metadata={"reason_code": KEYSET_SCHEMA_REQUIRED},
                    envelope_metadata=tenant_enforcement_metadata,
                )
            if (
                keyset_schema_load.max_schema_age_seconds is not None
                and keyset_schema_ttl_seconds > 0
                and keyset_schema_load.max_schema_age_seconds > keyset_schema_ttl_seconds
            ):
                tenant_enforcement_metadata["pagination.keyset.schema_stale"] = True
                tenant_enforcement_metadata["pagination.keyset.rejection_reason_code"] = (
                    KEYSET_SCHEMA_STALE
                )
                return _construct_error_response(
                    execution_started_at,
                    message=(
                        "Keyset pagination schema metadata is stale under strict schema mode."
                    ),
                    category=ErrorCategory.INVALID_REQUEST,
                    provider=provider,
                    metadata={"reason_code": KEYSET_SCHEMA_STALE},
                    envelope_metadata=tenant_enforcement_metadata,
                )

        if keyset_column_metadata:
            keyset_schema_info = StaticSchemaInfoProvider.from_column_metadata(
                keyset_column_metadata,
                table_names=keyset_table_names,
                unique_keys_by_table=keyset_unique_keys_by_table,
            )
            tenant_enforcement_metadata["pagination.keyset.schema_used"] = True
        try:
            validate_stable_tiebreaker(
                keyset_order_keys,
                table_names=keyset_table_names,
                allowlist=_keyset_tiebreaker_allowlist(),
                column_metadata=keyset_column_metadata,
                schema_info=keyset_schema_info,
            )
        except ValueError as e:
            error_message = str(e)
            reason_code = (
                KEYSET_REQUIRES_STABLE_TIEBREAKER
                if KEYSET_REQUIRES_STABLE_TIEBREAKER in error_message
                else "execution_pagination_keyset_invalid_sql"
            )
            for schema_reason in (
                KEYSET_ORDER_COLUMN_NOT_FOUND,
                KEYSET_SCHEMA_REQUIRED,
                KEYSET_SCHEMA_STALE,
                KEYSET_TIEBREAKER_NULLABLE,
                KEYSET_TIEBREAKER_NOT_UNIQUE,
            ):
                if schema_reason in error_message:
                    reason_code = schema_reason
                    break
            tenant_enforcement_metadata["pagination.keyset.rejection_reason_code"] = reason_code
            return _construct_error_response(
                execution_started_at,
                message=error_message,
                category=ErrorCategory.INVALID_REQUEST,
                provider=provider,
                metadata={"reason_code": reason_code},
                envelope_metadata=tenant_enforcement_metadata,
            )
        keyset_order_signature = build_keyset_order_signature(keyset_order_keys)

        if page_size:
            row_limit = page_size

        if keyset_cursor and len(keyset_cursor) > _DEFAULT_PAGE_TOKEN_MAX_LENGTH:
            return _construct_error_response(
                execution_started_at,
                message="Keyset cursor exceeds maximum allowed length.",
                category=ErrorCategory.INVALID_REQUEST,
                provider=provider,
                metadata={"reason_code": "execution_pagination_keyset_cursor_too_long"},
                envelope_metadata=tenant_enforcement_metadata,
            )
        if keyset_order_by and len(keyset_order_by) > 10:  # Bounded
            return _construct_error_response(
                execution_started_at,
                message="Keyset order-by columns exceed maximum allowed count.",
                category=ErrorCategory.INVALID_REQUEST,
                provider=provider,
                metadata={"reason_code": "execution_pagination_keyset_order_by_too_many_columns"},
                envelope_metadata=tenant_enforcement_metadata,
            )
    elif pagination_mode == "offset":
        if keyset_cursor or keyset_order_by:
            return _construct_error_response(
                execution_started_at,
                message="Keyset pagination fields are not allowed in offset mode.",
                category=ErrorCategory.INVALID_REQUEST,
                provider=provider,
                metadata={"reason_code": "execution_pagination_keyset_fields_not_allowed"},
                envelope_metadata=tenant_enforcement_metadata,
            )

    validation_error = _validate_sql_ast(sql_query, provider)
    if validation_error:
        validation_failure = _validate_sql_ast_failure(sql_query, provider)
        if validation_failure is None:
            validation_failure = SQLASTValidationFailure(
                message=validation_error,
                reason_code="sql_validation_error",
            )
        if validation_failure.reason_code in {
            "statement_type_not_allowed",
            "blocked_statement",
            "readonly_violation",
        }:
            emit_audit_event(
                AuditEventType.READONLY_VIOLATION,
                source=AuditEventSource.MCP,
                tenant_id=tenant_id,
                error_category=ErrorCategory.INVALID_REQUEST,
                metadata={
                    "provider": provider,
                    "reason_code": f"ast_{validation_failure.reason_code}",
                    "decision": "reject",
                },
            )
        return _construct_error_response(
            execution_started_at,
            validation_failure.message,
            category=validation_failure.category,
            provider=provider,
            metadata={
                "reason_code": validation_failure.reason_code,
                "error_code": validation_failure.error_code,
            },
            envelope_metadata=tenant_enforcement_metadata,
        )

    # 2.5 Complexity Guard
    complexity_error, complexity_metadata = _validate_sql_complexity(sql_query, provider)
    if complexity_error:
        emit_audit_event(
            AuditEventType.SQL_COMPLEXITY_REJECTION,
            source=AuditEventSource.MCP,
            tenant_id=tenant_id,
            error_category=ErrorCategory.INVALID_REQUEST,
            metadata={
                "provider": provider,
                "reason_code": "sql_complexity_limit_exceeded",
                "decision": "reject",
                "limit_name": (complexity_metadata or {}).get("complexity_limit_name"),
                "limit_value": (complexity_metadata or {}).get("complexity_limit_value"),
                "limit_measured": (complexity_metadata or {}).get("complexity_limit_measured"),
            },
        )
        return _construct_error_response(
            execution_started_at,
            complexity_error,
            category=ErrorCategory.INVALID_REQUEST,
            provider=provider,
            metadata=complexity_metadata,
            envelope_metadata=tenant_enforcement_metadata,
        )

    # 4. Final Safety Guardrail
    # Even if the provider claims to be read-only, we enforce it at the SQL level
    # to prevent driver bugs or misconfigurations from allowing writes.
    from dal.util.read_only import enforce_read_only_sql

    try:
        enforce_read_only_sql(sql_query, provider, read_only=True)
    except PermissionError as e:
        # Emit telemetry for blocked mutation
        span = trace.get_current_span()
        if span and span.is_recording():
            try:
                # Basic best-effort extraction since we might not have AST yet
                statement_type = (
                    sql_query.strip().split()[0].upper() if sql_query.strip() else "UNKNOWN"
                )
            except Exception:
                statement_type = "UNKNOWN"

            span.add_event(
                "mcp.read_only.blocked",
                attributes={
                    "provider": provider,
                    "category": ErrorCategory.MUTATION_BLOCKED.value,
                    "statement_type": statement_type,
                },
            )

        return _construct_error_response(
            execution_started_at,
            str(e),
            category=ErrorCategory.MUTATION_BLOCKED,
            provider=provider,
            envelope_metadata=tenant_enforcement_metadata,
        )

    # 3. Policy Enforcement (Table Allowlist & Sensitive Columns)
    from agent.validation.policy_enforcer import PolicyEnforcer

    try:
        PolicyEnforcer.validate_sql(sql_query)
    except ValueError as e:
        return _construct_error_response(
            execution_started_at,
            str(e),
            category=ErrorCategory.INVALID_REQUEST,
            provider=provider,
            envelope_metadata=tenant_enforcement_metadata,
        )

    # 1.5 Parameter Validation
    param_error = _validate_params(params)
    if param_error:
        return _construct_error_response(
            execution_started_at,
            param_error,
            category=ErrorCategory.INVALID_REQUEST,
            provider=provider,
            envelope_metadata=tenant_enforcement_metadata,
        )

    policy_decision = await policy.evaluate(
        sql=effective_sql_query,
        tenant_id=tenant_id,
        params=effective_params,
        tenant_column=_tenant_column_name(),
        global_table_allowlist=_tenant_global_table_allowlist(),
        schema_snapshot_loader=_load_table_columns_for_rewrite,
    )
    _record_policy_decision_telemetry(policy_decision.telemetry_attributes)
    tenant_enforcement_metadata.update(policy_decision.envelope_metadata)
    effective_sql_query = policy_decision.sql_to_execute
    effective_params = list(policy_decision.params_to_bind)
    if not policy_decision.should_execute:
        return _tenant_enforcement_unsupported_response(
            execution_started_at,
            provider,
            policy_decision=policy_decision,
            envelope_metadata=tenant_enforcement_metadata,
        )

    def _unsupported_capability_response(
        required_capability: str,
        provider_name: str,
        negotiation: Optional[CapabilityNegotiationResult] = None,
    ) -> str:
        """Construct an unsupported capability error response."""
        capability_required = (
            negotiation.capability_required if negotiation else required_capability
        )
        capability_supported = negotiation.capability_supported if negotiation else False
        fallback_policy = negotiation.fallback_policy if negotiation else "off"
        fallback_applied = negotiation.fallback_applied if negotiation else False
        fallback_mode = negotiation.fallback_mode if negotiation else "none"
        emit_audit_event(
            AuditEventType.POLICY_REJECTION,
            source=AuditEventSource.MCP,
            tenant_id=tenant_id,
            error_category=ErrorCategory.UNSUPPORTED_CAPABILITY,
            metadata={
                "provider": provider_name,
                "reason_code": "capability_denied",
                "decision": "reject",
                "required_capability": required_capability,
                "capability_supported": bool(capability_supported),
                "fallback_policy": fallback_policy,
                "fallback_applied": bool(fallback_applied),
                "fallback_mode": fallback_mode,
            },
        )

        return _construct_error_response(
            execution_started_at,
            message=f"Requested capability is not supported: {required_capability}.",
            category=ErrorCategory.UNSUPPORTED_CAPABILITY,
            provider=provider_name,
            metadata={
                "required_capability": required_capability,
                "capability_required": capability_required,
                "capability_supported": capability_supported,
                "fallback_policy": fallback_policy,
                "fallback_applied": fallback_applied,
                "fallback_mode": fallback_mode,
            },
            envelope_metadata=tenant_enforcement_metadata,
        )

    fallback_policy = parse_capability_fallback_policy(
        get_env_str("AGENT_CAPABILITY_FALLBACK_MODE")
    )
    capability_metadata = {
        "capability_required": None,
        "capability_supported": True,
        "fallback_applied": False,
        "fallback_mode": "none",
    }
    cap_mitigation_setting = (get_env_str("AGENT_PROVIDER_CAP_MITIGATION", "off") or "off").strip()
    cap_mitigation_setting = cap_mitigation_setting.lower()
    if cap_mitigation_setting not in {"off", "safe"}:
        cap_mitigation_setting = "off"
    force_result_limit = None

    def _negotiate_if_required(
        required_capability: str,
        required: bool,
        supported: bool,
    ) -> Optional[str]:
        nonlocal include_columns
        nonlocal timeout_seconds
        nonlocal page_token
        nonlocal page_size
        nonlocal capability_metadata
        nonlocal force_result_limit

        if not required:
            return None
        decision = negotiate_capability_request(
            capability_required=required_capability,
            capability_supported=supported,
            fallback_policy=fallback_policy,
            include_columns=include_columns,
            timeout_seconds=timeout_seconds,
            page_token=page_token,
            page_size=page_size,
        )
        capability_metadata = decision.to_metadata()
        include_columns = decision.include_columns
        timeout_seconds = decision.timeout_seconds
        page_token = decision.page_token
        page_size = decision.page_size
        if decision.force_result_limit is not None:
            force_result_limit = decision.force_result_limit
        if not decision.capability_supported and not decision.fallback_applied:
            return _unsupported_capability_response(required_capability, provider, decision)
        return None

    unsupported_response = _negotiate_if_required(
        "column_metadata",
        include_columns,
        caps.supports_column_metadata,
    )
    if unsupported_response is not None:
        return unsupported_response
    unsupported_response = _negotiate_if_required(
        "async_cancel",
        bool(
            effective_timeout_seconds
            and effective_timeout_seconds > 0
            and getattr(caps, "execution_model", "sync") == "async"
        ),
        bool(getattr(caps, "supports_cancel", False)),
    )
    if unsupported_response is not None:
        return unsupported_response
    supports_server_pagination = bool(getattr(caps, "supports_pagination", False))
    supports_offset_pagination_wrapper = bool(
        getattr(caps, "supports_offset_pagination_wrapper", False)
    )
    supports_query_wrapping_subselect = bool(
        getattr(caps, "supports_query_wrapping_subselect", False)
    )
    pagination_requested = bool(
        page_token or page_size or keyset_cursor or pagination_mode == "keyset"
    )
    execution_topology = _normalize_execution_topology(getattr(caps, "execution_topology", None))
    federated_ordering_supported = bool(
        getattr(caps, "supports_federated_deterministic_ordering", False)
    )
    if pagination_requested:
        tenant_enforcement_metadata["pagination.execution_topology"] = execution_topology
        tenant_enforcement_metadata["pagination.federated.ordering_supported"] = (
            federated_ordering_supported
        )
        tenant_enforcement_metadata["pagination.reject_reason_code"] = None
    if pagination_requested and not (
        supports_server_pagination
        or (supports_offset_pagination_wrapper and supports_query_wrapping_subselect)
    ):
        return _construct_error_response(
            execution_started_at,
            "Pagination is not supported for this provider.",
            category=ErrorCategory.INVALID_REQUEST,
            provider=provider,
            metadata={
                "reason_code": "execution_pagination_unsupported_provider",
                "required_capability": "pagination",
            },
            envelope_metadata=tenant_enforcement_metadata,
        )
    is_federated = execution_topology == "federated"
    if pagination_mode == "keyset" and is_federated and not federated_ordering_supported:
        return _construct_error_response(
            execution_started_at,
            "Keyset pagination is not supported for federated backends "
            "without deterministic ordering.",
            category=ErrorCategory.INVALID_REQUEST,
            provider=provider,
            metadata={
                "reason_code": "PAGINATION_FEDERATED_ORDERING_UNSAFE",
                "required_capability": "federated_deterministic_ordering",
            },
            envelope_metadata=tenant_enforcement_metadata,
        )

    disallow_federated_offset = get_env_bool("PAGINATION_DISALLOW_FEDERATED_OFFSET", False)
    if pagination_mode == "offset" and is_federated and disallow_federated_offset:
        return _construct_error_response(
            execution_started_at,
            "Offset pagination is not supported for federated backends.",
            category=ErrorCategory.INVALID_REQUEST,
            provider=provider,
            metadata={
                "reason_code": "PAGINATION_FEDERATED_UNSUPPORTED",
                "required_capability": "deterministic_federated_ordering",
            },
            envelope_metadata=tenant_enforcement_metadata,
        )

    # Cursor guardrail env vars — all fail-closed with safe defaults.
    # See dal/pagination_cursor.py module docstring for full semantics.
    max_page_token_len = get_env_int(
        "EXECUTION_PAGINATION_TOKEN_MAX_LENGTH", _DEFAULT_PAGE_TOKEN_MAX_LENGTH
    )
    cursor_max_age_seconds = max(
        1, int(get_env_int("PAGINATION_CURSOR_MAX_AGE_SECONDS", 3600) or 3600)
    )
    cursor_clock_skew_seconds = max(
        0, int(get_env_int("PAGINATION_CURSOR_CLOCK_SKEW_SECONDS", 300) or 300)
    )
    cursor_require_issued_at = get_env_bool("PAGINATION_CURSOR_REQUIRE_ISSUED_AT", True)
    cursor_bind_query_fingerprint = get_env_bool(
        "PAGINATION_CURSOR_BIND_QUERY_FINGERPRINT", pagination_mode == "keyset"
    )
    max_offset_pages = max(
        1,
        int(
            get_env_int(
                "EXECUTION_PAGINATION_MAX_OFFSET_PAGES", _DEFAULT_PAGINATION_MAX_OFFSET_PAGES
            )
            or _DEFAULT_PAGINATION_MAX_OFFSET_PAGES
        ),
    )
    try:
        pagination_token_secret = resolve_cursor_signing_secret()
    except CursorSigningSecretMissing:
        pagination_token_secret = None
        _pagination_signing_available = False
    else:
        _pagination_signing_available = True
    tenant_enforcement_metadata["pagination.cursor.signing_secret_configured"] = (
        _pagination_signing_available
    )
    if page_token is not None:
        normalized_page_token = page_token.strip()
        if not normalized_page_token:
            return _construct_error_response(
                execution_started_at,
                "Invalid page_token: must not be empty.",
                category=ErrorCategory.INVALID_REQUEST,
                provider=provider,
                metadata={"reason_code": "execution_pagination_page_token_invalid"},
                envelope_metadata=tenant_enforcement_metadata,
            )
        if len(normalized_page_token) > max_page_token_len:
            return _construct_error_response(
                execution_started_at,
                "Invalid page_token: exceeds maximum length.",
                category=ErrorCategory.INVALID_REQUEST,
                provider=provider,
                metadata={"reason_code": "execution_pagination_page_token_too_long"},
                envelope_metadata=tenant_enforcement_metadata,
            )
        page_token = normalized_page_token

    max_page_size = (
        max(1, int(resource_limits.max_rows)) if resource_limits.enforce_row_limit else 1000
    )
    if page_size is not None:
        if page_size <= 0:
            return _construct_error_response(
                execution_started_at,
                "Invalid page_size: must be greater than zero.",
                category=ErrorCategory.INVALID_REQUEST,
                provider=provider,
                metadata={"reason_code": "execution_pagination_page_size_invalid"},
                envelope_metadata=tenant_enforcement_metadata,
            )
        if page_size > max_page_size:
            return _construct_error_response(
                execution_started_at,
                "Invalid page_size: exceeds maximum rows per request.",
                category=ErrorCategory.INVALID_REQUEST,
                provider=provider,
                metadata={"reason_code": "execution_pagination_page_size_exceeds_max_rows"},
                envelope_metadata=tenant_enforcement_metadata,
            )

    if provider == "redshift":
        from dal.redshift import validate_redshift_query

        errors = validate_redshift_query(effective_sql_query)
        if errors:
            return _construct_error_response(
                execution_started_at,
                "Redshift query validation failed.",
                category=ErrorCategory.INVALID_REQUEST,
                provider=provider,
                metadata={"details": errors},
                envelope_metadata=tenant_enforcement_metadata,
            )

    try:
        columns = None
        last_truncated = False
        row_limit = 0
        bytes_returned = 0
        execution_duration_ms = 0
        next_token = None
        applied_page_size = page_size
        conn = None
        offset_decode_metadata: dict[str, Any] | None = None
        query_fingerprint = None  # Bound to backend signature inside connection block
        cursor_query_fingerprint = None

        async with Database.get_connection(tenant_id=tenant_id, read_only=True) as conn:
            keyset_cursor_context = _extract_keyset_cursor_context(conn)
            backend_set_sig = _extract_backend_set_signature(
                conn,
                keyset_cursor_context=keyset_cursor_context,
            )
            if backend_set_sig:
                keyset_cursor_context["backend_set_sig"] = backend_set_sig
            if pagination_mode == "keyset":
                tenant_enforcement_metadata["pagination.backend_set_sig_present"] = bool(
                    backend_set_sig
                )
            partition_signature = keyset_cursor_context.get("partition_signature")
            backend_signature = backend_set_sig or partition_signature

            keyset_signature_for_fingerprint = (
                json.dumps(keyset_order_signature, separators=(",", ":"))
                if keyset_order_signature
                else None
            )
            cursor_query_fingerprint = build_cursor_query_fingerprint(
                sql=effective_sql_query,
                provider=provider,
                pagination_mode=pagination_mode,
                order_signature=keyset_signature_for_fingerprint,
            )
            query_fingerprint = build_query_fingerprint(
                sql=effective_sql_query,
                params=effective_params,
                tenant_id=tenant_id,
                provider=provider,
                max_rows=int(resource_limits.max_rows),
                max_bytes=int(resource_limits.max_bytes),
                max_execution_ms=int(resource_limits.max_execution_ms),
                order_signature=keyset_signature_for_fingerprint,
                backend_signature=backend_signature,
            )
            raw_session_guardrail_metadata = getattr(conn, "session_guardrail_metadata", {})
            if isinstance(raw_session_guardrail_metadata, dict):
                session_guardrail_metadata = {
                    key: raw_session_guardrail_metadata.get(key)
                    for key in (
                        "session_guardrail_applied",
                        "session_guardrail_outcome",
                        "execution_role_applied",
                        "execution_role_name",
                        "restricted_session_mode",
                        "session_guardrail_capability_mismatch",
                    )
                }
            tenant_enforcement_metadata.update(session_guardrail_metadata)
            tenant_enforcement_metadata.update(_extract_postgres_sandbox_metadata(conn))
            row_limit = _resolve_row_limit(conn)
            effective_page_size = page_size
            if effective_page_size and effective_page_size > row_limit and row_limit:
                effective_page_size = row_limit
            if effective_page_size and effective_page_size > max_page_size:
                effective_page_size = max_page_size
            applied_page_size = effective_page_size
            if pagination_mode == "keyset":
                import sqlglot

                from dal.keyset_pagination import (
                    KEYSET_ORDER_MISMATCH,
                    KEYSET_PARTITION_SET_CHANGED,
                    KEYSET_SHARD_MISMATCH,
                    KEYSET_SNAPSHOT_MISMATCH,
                    KEYSET_TOPOLOGY_MISMATCH,
                    PAGINATION_BACKEND_SET_CHANGED,
                    apply_keyset_pagination,
                    canonicalize_keyset_sql,
                    decode_keyset_cursor,
                )

                # Already extracted context above for fingerprint
                keyset_db_role = keyset_cursor_context.get("db_role")
                keyset_region = keyset_cursor_context.get("region")
                keyset_node_id = keyset_cursor_context.get("node_id")
                keyset_shard_id = keyset_cursor_context.get("shard_id")
                keyset_shard_key_hash = keyset_cursor_context.get("shard_key_hash")
                keyset_partition_signature = keyset_cursor_context.get("partition_signature")
                keyset_backend_set_sig = keyset_cursor_context.get("backend_set_sig")
                tenant_enforcement_metadata["pagination.keyset.snapshot_id_present"] = bool(
                    keyset_cursor_context.get("snapshot_id")
                )
                tenant_enforcement_metadata["pagination.backend_set_sig_present"] = bool(
                    keyset_backend_set_sig
                )
                tenant_enforcement_metadata["pagination.keyset.db_role"] = (
                    keyset_db_role if keyset_db_role in {"primary", "replica"} else "unknown"
                )
                tenant_enforcement_metadata["pagination.keyset.region"] = (
                    keyset_region if keyset_region else "unknown"
                )
                tenant_enforcement_metadata["pagination.keyset.node_id_present"] = bool(
                    keyset_node_id
                )
                tenant_enforcement_metadata["pagination.keyset.topology_available"] = bool(
                    keyset_db_role or keyset_region or keyset_node_id
                )
                tenant_enforcement_metadata["pagination.keyset.shard_id"] = (
                    keyset_shard_id if keyset_shard_id else "unknown"
                )
                tenant_enforcement_metadata["pagination.keyset.shard_key_hash_present"] = bool(
                    keyset_shard_key_hash
                )
                tenant_enforcement_metadata["pagination.keyset.shard_info_available"] = bool(
                    keyset_shard_id or keyset_shard_key_hash
                )
                tenant_enforcement_metadata["pagination.keyset.partition_signature"] = (
                    keyset_partition_signature if keyset_partition_signature else "unknown"
                )
                tenant_enforcement_metadata["pagination.keyset.partition_signature_available"] = (
                    bool(keyset_partition_signature)
                )
                if keyset_topology_strict and not tenant_enforcement_metadata.get(
                    "pagination.keyset.topology_available"
                ):
                    tenant_enforcement_metadata["pagination.keyset.rejection_reason_code"] = (
                        "KEYSET_TOPOLOGY_REQUIRED"
                    )
                    return _construct_error_response(
                        execution_started_at,
                        message=(
                            "Keyset pagination requires execution topology metadata when strict "
                            "topology mode is enabled."
                        ),
                        category=ErrorCategory.INVALID_REQUEST,
                        provider=provider,
                        metadata={"reason_code": "KEYSET_TOPOLOGY_REQUIRED"},
                        envelope_metadata=tenant_enforcement_metadata,
                    )
                replica_lag_seconds = await _extract_keyset_replica_lag_seconds(conn)
                if replica_lag_seconds is not None:
                    tenant_enforcement_metadata["pagination.keyset.replica_lag_seconds"] = (
                        replica_lag_seconds
                    )
                if (
                    keyset_db_role == "replica"
                    and keyset_max_replica_lag_seconds > 0
                    and replica_lag_seconds is not None
                    and replica_lag_seconds > keyset_max_replica_lag_seconds
                ):
                    tenant_enforcement_metadata["pagination.keyset.rejection_reason_code"] = (
                        "KEYSET_REPLICA_LAG_UNSAFE"
                    )
                    return _construct_error_response(
                        execution_started_at,
                        message=(
                            "Keyset pagination is not safe on replicas with excessive "
                            "replication lag."
                        ),
                        category=ErrorCategory.INVALID_REQUEST,
                        provider=provider,
                        metadata={"reason_code": "KEYSET_REPLICA_LAG_UNSAFE"},
                        envelope_metadata=tenant_enforcement_metadata,
                    )
                if keyset_snapshot_strict and not keyset_cursor_context.get("snapshot_id"):
                    tenant_enforcement_metadata["pagination.keyset.rejection_reason_code"] = (
                        "KEYSET_SNAPSHOT_REQUIRED"
                    )
                    return _construct_error_response(
                        execution_started_at,
                        message=(
                            "Keyset pagination requires snapshot identifiers when strict snapshot "
                            "mode is enabled."
                        ),
                        category=ErrorCategory.INVALID_REQUEST,
                        provider=provider,
                        metadata={"reason_code": "KEYSET_SNAPSHOT_REQUIRED"},
                        envelope_metadata=tenant_enforcement_metadata,
                    )
                keyset_isolation_level = _extract_keyset_isolation_level(conn, caps)
                if keyset_isolation_level is not None:
                    tenant_enforcement_metadata["pagination.keyset.isolation_level"] = (
                        keyset_isolation_level
                    )
                if (
                    not keyset_allow_weaker_isolation
                    and keyset_isolation_level is not None
                    and not _is_keyset_isolation_safe(keyset_isolation_level)
                ):
                    tenant_enforcement_metadata["pagination.keyset.rejection_reason_code"] = (
                        "KEYSET_ISOLATION_UNSAFE"
                    )
                    return _construct_error_response(
                        execution_started_at,
                        message=(
                            "Keyset pagination requires REPEATABLE READ or stronger isolation."
                        ),
                        category=ErrorCategory.INVALID_REQUEST,
                        provider=provider,
                        metadata={"reason_code": "KEYSET_ISOLATION_UNSAFE"},
                        envelope_metadata=tenant_enforcement_metadata,
                    )

                expected_cursor_context = keyset_cursor_context
                if keyset_allow_cross_shard and keyset_cursor_context:
                    expected_cursor_context = {
                        key: value
                        for key, value in keyset_cursor_context.items()
                        if key not in {"shard_id", "shard_key_hash"}
                    }

                if keyset_cursor:
                    if not _pagination_signing_available:
                        return _construct_error_response(
                            execution_started_at,
                            "Cursor signing secret is not configured.",
                            category=ErrorCategory.INVALID_REQUEST,
                            provider=provider,
                            metadata={"reason_code": PAGINATION_CURSOR_SECRET_MISSING},
                            envelope_metadata=tenant_enforcement_metadata,
                        )
                    keyset_decode_metadata: dict[str, Any] = {}
                    try:
                        keyset_values = decode_keyset_cursor(
                            keyset_cursor,
                            expected_fingerprint=query_fingerprint,
                            secret=pagination_token_secret,
                            expected_keys=keyset_order_signature,
                            expected_cursor_context=expected_cursor_context or None,
                            require_issued_at=cursor_require_issued_at,
                            decode_metadata=keyset_decode_metadata,
                            max_age_seconds=cursor_max_age_seconds,
                            clock_skew_seconds=cursor_clock_skew_seconds,
                            expected_query_fp=(
                                cursor_query_fingerprint if cursor_bind_query_fingerprint else None
                            ),
                        )
                    except ValueError as e:
                        reason_code = "execution_pagination_keyset_cursor_invalid"
                        if KEYSET_ORDER_MISMATCH in str(e):
                            reason_code = KEYSET_ORDER_MISMATCH
                        elif KEYSET_SNAPSHOT_MISMATCH in str(e):
                            reason_code = KEYSET_SNAPSHOT_MISMATCH
                            tenant_enforcement_metadata["pagination.keyset.snapshot_mismatch"] = (
                                True
                            )
                        elif KEYSET_TOPOLOGY_MISMATCH in str(e):
                            reason_code = KEYSET_TOPOLOGY_MISMATCH
                            tenant_enforcement_metadata["pagination.keyset.topology_mismatch"] = (
                                True
                            )
                        elif KEYSET_SHARD_MISMATCH in str(e):
                            reason_code = KEYSET_SHARD_MISMATCH
                            tenant_enforcement_metadata["pagination.keyset.shard_mismatch"] = True
                        elif KEYSET_PARTITION_SET_CHANGED in str(e):
                            reason_code = KEYSET_PARTITION_SET_CHANGED
                            tenant_enforcement_metadata[
                                "pagination.keyset.partition_set_changed"
                            ] = True
                        elif PAGINATION_BACKEND_SET_CHANGED in str(e):
                            reason_code = PAGINATION_BACKEND_SET_CHANGED
                            tenant_enforcement_metadata["pagination.backend_set_mismatch"] = True
                        elif "PAGINATION_CURSOR_EXPIRED" in str(e):
                            reason_code = "PAGINATION_CURSOR_EXPIRED"
                        elif "PAGINATION_CURSOR_ISSUED_AT_INVALID" in str(e):
                            reason_code = "PAGINATION_CURSOR_ISSUED_AT_INVALID"
                        elif "PAGINATION_CURSOR_CLOCK_SKEW" in str(e):
                            reason_code = "PAGINATION_CURSOR_CLOCK_SKEW"
                        elif "PAGINATION_CURSOR_QUERY_MISMATCH" in str(e):
                            reason_code = "PAGINATION_CURSOR_QUERY_MISMATCH"
                        elif "PAGINATION_CURSOR_SIGNATURE_INVALID" in str(e):
                            reason_code = "PAGINATION_CURSOR_SIGNATURE_INVALID"
                            tenant_enforcement_metadata["pagination.cursor.signature_valid"] = False
                        _apply_cursor_decode_metadata(
                            tenant_enforcement_metadata,
                            keyset_decode_metadata,
                            fallback_reason_code=reason_code,
                        )
                        tenant_enforcement_metadata["pagination.keyset.rejection_reason_code"] = (
                            reason_code
                        )
                        return _construct_error_response(
                            execution_started_at,
                            message=str(e),
                            category=ErrorCategory.INVALID_REQUEST,
                            provider=provider,
                            metadata={"reason_code": reason_code},
                            envelope_metadata=tenant_enforcement_metadata,
                        )
                    if pagination_token_secret:
                        tenant_enforcement_metadata["pagination.cursor.signature_valid"] = True
                    _apply_cursor_decode_metadata(
                        tenant_enforcement_metadata, keyset_decode_metadata
                    )
                    if len(keyset_values) != len(keyset_order_keys):
                        return _construct_error_response(
                            execution_started_at,
                            message="Keyset cursor value count mismatch with ORDER BY columns.",
                            category=ErrorCategory.INVALID_REQUEST,
                            provider=provider,
                            metadata={
                                "reason_code": "execution_pagination_keyset_cursor_column_mismatch"
                            },
                            envelope_metadata=tenant_enforcement_metadata,
                        )

                try:
                    dialect = normalize_sqlglot_dialect(provider)
                    parsed_effective = sqlglot.parse_one(effective_sql_query, read=dialect)
                    if not isinstance(parsed_effective, sqlglot.exp.Select):
                        raise ValueError("Effective query is not a SELECT statement.")
                    execution_select = parsed_effective
                    if keyset_values:
                        execution_select = apply_keyset_pagination(
                            parsed_effective,
                            keyset_order_keys,
                            keyset_values,
                            provider=provider,
                        )
                    keyset_rewritten_select = execution_select
                except Exception as e:
                    return _construct_error_response(
                        execution_started_at,
                        message=f"Failed to apply keyset pagination rewrite: {str(e)}",
                        category=ErrorCategory.INTERNAL,
                        provider=provider,
                        envelope_metadata=tenant_enforcement_metadata,
                    )

                execution_select = keyset_rewritten_select.copy()
                if effective_page_size:
                    execution_select = execution_select.limit(int(effective_page_size) + 1)
                effective_sql_query = canonicalize_keyset_sql(execution_select, provider=provider)
                if effective_page_size is not None:
                    effective_page_size_int = int(effective_page_size)
                    tenant_enforcement_metadata["pagination.keyset.effective_page_size"] = (
                        effective_page_size_int
                    )
                    tenant_enforcement_metadata["pagination.keyset.page_size_effective"] = (
                        effective_page_size_int
                    )

            async def _fetch_rows():
                """Fetch rows from the database."""
                nonlocal columns, next_token, offset_decode_metadata
                fetch_page = getattr(conn, "fetch_page", None)
                fetch_page_with_columns = getattr(conn, "fetch_page_with_columns", None)
                offset_pagination_requested = pagination_mode == "offset" and bool(
                    page_token or effective_page_size
                )
                if (
                    offset_pagination_requested
                    and supports_server_pagination
                    and callable(fetch_page)
                ):
                    if include_columns and callable(fetch_page_with_columns):
                        rows, columns, next_token = await fetch_page_with_columns(
                            effective_sql_query,
                            page_token,
                            effective_page_size,
                            *effective_params,
                        )
                        return rows
                    rows, next_token = await fetch_page(
                        effective_sql_query,
                        page_token,
                        effective_page_size,
                        *effective_params,
                    )
                    return rows
                if offset_pagination_requested and (
                    not callable(fetch_page) or not supports_server_pagination
                ):
                    if not (
                        supports_offset_pagination_wrapper and supports_query_wrapping_subselect
                    ):
                        raise OffsetPaginationTokenError(
                            reason_code="execution_pagination_unsupported_provider",
                            message="Pagination is not supported for this provider.",
                        )
                    if _query_contains_limit_or_offset(effective_sql_query, provider):
                        raise OffsetPaginationTokenError(
                            reason_code="execution_pagination_sql_contains_limit_offset",
                            message=(
                                "Pagination is not supported for SQL with LIMIT/OFFSET clauses."
                            ),
                        )
                    pagination_offset = 0
                    pagination_limit = int(effective_page_size or 0)
                    if page_token:
                        if not _pagination_signing_available:
                            raise OffsetPaginationTokenError(
                                reason_code=PAGINATION_CURSOR_SECRET_MISSING,
                                message="Cursor signing secret is not configured.",
                            )
                        offset_decode_metadata = {}
                        token_payload = decode_offset_pagination_token(
                            token=page_token,
                            expected_fingerprint=query_fingerprint,
                            max_length=max_page_token_len,
                            secret=pagination_token_secret or None,
                            require_issued_at=cursor_require_issued_at,
                            decode_metadata=offset_decode_metadata,
                            max_age_seconds=cursor_max_age_seconds,
                            clock_skew_seconds=cursor_clock_skew_seconds,
                            expected_query_fp=(
                                cursor_query_fingerprint if cursor_bind_query_fingerprint else None
                            ),
                        )
                        pagination_offset = token_payload.offset
                        pagination_limit = token_payload.limit
                        if pagination_token_secret:
                            tenant_enforcement_metadata["pagination.cursor.signature_valid"] = True
                        _apply_cursor_decode_metadata(
                            tenant_enforcement_metadata, offset_decode_metadata
                        )
                        if effective_page_size is not None and int(effective_page_size) != int(
                            token_payload.limit
                        ):
                            raise OffsetPaginationTokenError(
                                reason_code="execution_pagination_page_size_mismatch",
                                message=(
                                    "Pagination token limit does not match requested page_size."
                                ),
                            )
                    if pagination_limit <= 0:
                        raise OffsetPaginationTokenError(
                            reason_code="execution_pagination_page_size_invalid",
                            message="Pagination requires a positive page_size.",
                        )
                    if pagination_offset > max_offset_pages * pagination_limit:
                        raise OffsetPaginationTokenError(
                            reason_code="execution_pagination_offset_exceeds_limit",
                            message="Pagination offset exceeds configured bounds.",
                        )
                    wrapped_limit = pagination_limit + 1
                    wrapped_sql = (
                        f"SELECT * FROM ({effective_sql_query}) AS text2sql_page "
                        f"LIMIT {wrapped_limit} OFFSET {pagination_offset}"
                    )
                    if effective_params:
                        rows = await conn.fetch(wrapped_sql, *effective_params)
                    else:
                        rows = await conn.fetch(wrapped_sql)
                    rows = [dict(row) for row in rows]
                    if len(rows) > pagination_limit:
                        rows = rows[:pagination_limit]
                        if not _pagination_signing_available:
                            raise OffsetPaginationTokenError(
                                reason_code=PAGINATION_CURSOR_SECRET_MISSING,
                                message="Cursor signing secret is not configured.",
                            )
                        next_token = encode_offset_pagination_token(
                            offset=pagination_offset + pagination_limit,
                            limit=pagination_limit,
                            fingerprint=query_fingerprint,
                            secret=pagination_token_secret or None,
                            max_age_s=cursor_max_age_seconds,
                            query_fp=(
                                cursor_query_fingerprint if cursor_bind_query_fingerprint else None
                            ),
                        )
                    else:
                        next_token = None
                    return rows
                if include_columns:
                    fetch_with_columns = getattr(conn, "fetch_with_columns", None)
                    prepare = getattr(conn, "prepare", None)
                    supports_fetch_with_columns = (
                        callable(fetch_with_columns) and "fetch_with_columns" in type(conn).__dict__
                    )
                    supports_prepare = callable(prepare) and "prepare" in type(conn).__dict__
                    if effective_params:
                        if supports_fetch_with_columns:
                            rows, columns = await fetch_with_columns(
                                effective_sql_query, *effective_params
                            )
                        elif supports_prepare:
                            from dal.util.column_metadata import columns_from_asyncpg_attributes

                            statement = await prepare(effective_sql_query)
                            rows = await statement.fetch(*effective_params)
                            columns = columns_from_asyncpg_attributes(statement.get_attributes())
                            rows = [dict(row) for row in rows]
                        else:
                            rows = await conn.fetch(effective_sql_query, *effective_params)
                            rows = [dict(row) for row in rows]
                    else:
                        if supports_fetch_with_columns:
                            rows, columns = await fetch_with_columns(effective_sql_query)
                        elif supports_prepare:
                            from dal.util.column_metadata import columns_from_asyncpg_attributes

                            statement = await prepare(effective_sql_query)
                            rows = await statement.fetch()
                            columns = columns_from_asyncpg_attributes(statement.get_attributes())
                            rows = [dict(row) for row in rows]
                        else:
                            rows = await conn.fetch(effective_sql_query)
                            rows = [dict(row) for row in rows]
                else:
                    if effective_params:
                        rows = await conn.fetch(effective_sql_query, *effective_params)
                    else:
                        rows = await conn.fetch(effective_sql_query)
                    rows = [dict(row) for row in rows]
                return rows

            try:
                result_rows = await run_with_timeout(
                    _fetch_rows,
                    effective_timeout_seconds,
                    cancel=lambda: _cancel_best_effort(conn),
                    provider=provider,
                    operation_name="execute_sql_query.fetch",
                )
            except (asyncio.TimeoutError, TimeoutError) as timeout_exc:
                tenant_enforcement_metadata["execution_timeout_triggered"] = True
                raise _SandboxExecutionTimeout("Execution timed out.") from timeout_exc

            raw_last_truncated = getattr(conn, "last_truncated", False)
            last_truncated = raw_last_truncated if isinstance(raw_last_truncated, bool) else False
            raw_reason = getattr(conn, "last_truncated_reason", None)
            last_truncated_reason = raw_reason if isinstance(raw_reason, str) else None
            if pagination_mode == "keyset" and streaming:
                raw_streaming_terminated = getattr(conn, "last_streaming_terminated", False)
                raw_client_disconnected = getattr(conn, "last_stream_client_disconnected", False)
                streaming_terminated_early = bool(raw_streaming_terminated) or bool(
                    raw_client_disconnected
                )

        if conn is not None:
            tenant_enforcement_metadata.update(_extract_postgres_sandbox_metadata(conn))

        if pagination_mode == "keyset" and applied_page_size is not None and applied_page_size > 0:
            requested_page_size = int(applied_page_size)
            adaptive_page_size = requested_page_size
            byte_budget = max(0, int(resource_limits.max_bytes))
            if resource_limits.enforce_byte_limit and byte_budget > 0:
                average_row_size = _rolling_average_row_size_bytes(
                    result_rows[:requested_page_size]
                )
                estimated_row_size = average_row_size or _ADAPTIVE_ROW_SIZE_FALLBACK_BYTES
                budget_limited_page_size = max(1, byte_budget // max(1, int(estimated_row_size)))
                adaptive_page_size = min(requested_page_size, int(budget_limited_page_size))
            adaptive_page_size = max(1, int(adaptive_page_size))
            applied_page_size = adaptive_page_size
            tenant_enforcement_metadata["pagination.keyset.adaptive_page_size"] = adaptive_page_size
            tenant_enforcement_metadata["pagination.keyset.effective_page_size"] = (
                adaptive_page_size
            )
            tenant_enforcement_metadata["pagination.keyset.page_size_effective"] = (
                adaptive_page_size
            )

        keyset_page_truncated = False
        if (
            pagination_mode == "keyset"
            and applied_page_size is not None
            and applied_page_size > 0
            and len(result_rows) > int(applied_page_size)
        ):
            result_rows = result_rows[: int(applied_page_size)]
            keyset_page_truncated = True

        effective_row_limit = int(row_limit or 0)
        if resource_limits.enforce_row_limit:
            configured_row_limit = max(1, int(resource_limits.max_rows))
            effective_row_limit = (
                min(effective_row_limit, configured_row_limit)
                if effective_row_limit > 0
                else configured_row_limit
            )
        row_limit_result = enforce_row_limit(
            result_rows,
            max_rows=effective_row_limit,
            enforce=effective_row_limit > 0,
        )
        result_rows = row_limit_result.rows
        row_limit_truncated = row_limit_result.partial
        if effective_row_limit > 0:
            row_limit = effective_row_limit

        # Size Safety Valve
        safety_limit = 0 if resource_limits.enforce_row_limit else 1000
        safety_truncated = False
        if safety_limit > 0 and len(result_rows) > safety_limit:
            result_rows = result_rows[:safety_limit]
            safety_truncated = True
            row_limit = safety_limit
        forced_limited = False
        if (
            force_result_limit is not None
            and force_result_limit > 0
            and len(result_rows) > force_result_limit
        ):
            result_rows = result_rows[:force_result_limit]
            forced_limited = True
            row_limit = force_result_limit

        byte_limit_result = enforce_byte_limit(
            result_rows,
            max_bytes=int(resource_limits.max_bytes),
            enforce=resource_limits.enforce_byte_limit,
            envelope_overhead={"metadata": {}, "rows": []},
        )
        size_truncated = byte_limit_result.partial
        size_truncated_reason = byte_limit_result.partial_reason
        result_rows = byte_limit_result.rows
        bytes_returned = byte_limit_result.bytes_returned
        execution_duration_ms = max(0, int((time.monotonic() - execution_started_at) * 1000))
        if size_truncated:
            next_token = None
            if pagination_mode == "keyset" and streaming:
                streaming_terminated_early = True

        if include_columns and not columns:
            columns = _build_columns_from_rows(result_rows)

        is_truncated = bool(
            last_truncated
            or keyset_page_truncated
            or row_limit_truncated
            or safety_truncated
            or forced_limited
            or size_truncated
        )

        if pagination_mode == "keyset":
            # Keyset pagination does not use offset page tokens.
            next_token = None

        keyset_partial_page = bool(
            pagination_mode == "keyset" and (size_truncated or row_limit_truncated)
        )
        if pagination_mode == "keyset":
            tenant_enforcement_metadata["pagination.keyset.partial_page"] = keyset_partial_page
            if streaming:
                tenant_enforcement_metadata["pagination.keyset.streaming_terminated"] = (
                    streaming_terminated_early
                )
        if keyset_partial_page:
            tenant_enforcement_metadata["pagination.keyset.cursor_emitted"] = False
            tenant_enforcement_metadata["next_keyset_cursor"] = None
        if pagination_mode == "keyset" and streaming and streaming_terminated_early:
            tenant_enforcement_metadata["pagination.keyset.cursor_emitted"] = False
            tenant_enforcement_metadata["next_keyset_cursor"] = None

        if (
            pagination_mode == "keyset"
            and keyset_page_truncated
            and not keyset_partial_page
            and not streaming_terminated_early
            and result_rows
        ):
            from dal.keyset_pagination import encode_keyset_cursor, get_keyset_values

            emitted_row_count = len(result_rows)
            cursor_row_index = emitted_row_count - 1
            if cursor_row_index != emitted_row_count - 1:
                raise AssertionError(
                    "Invariant violation: cursor_row_index must equal emitted_row_count - 1."
                )
            try:
                if not _pagination_signing_available:
                    return _construct_error_response(
                        execution_started_at,
                        "Cursor signing secret is not configured.",
                        category=ErrorCategory.INVALID_REQUEST,
                        provider=provider,
                        metadata={"reason_code": PAGINATION_CURSOR_SECRET_MISSING},
                        envelope_metadata=tenant_enforcement_metadata,
                    )
                keyset_vals = get_keyset_values(result_rows[cursor_row_index], keyset_order_keys)
                next_keyset_cursor = encode_keyset_cursor(
                    keyset_vals,
                    keyset_order_signature,
                    query_fingerprint,
                    secret=pagination_token_secret,
                    cursor_context=keyset_cursor_context or None,
                    max_age_s=cursor_max_age_seconds,
                    query_fp=(cursor_query_fingerprint if cursor_bind_query_fingerprint else None),
                )
                tenant_enforcement_metadata["next_keyset_cursor"] = next_keyset_cursor
                tenant_enforcement_metadata["pagination.keyset.cursor_emitted"] = True
                tenant_enforcement_metadata["is_paginated"] = True
            except Exception as e:
                logger.warning(f"Failed to generate next_keyset_cursor: {e}")
        if row_limit is not None and row_limit > 0:
            tenant_enforcement_metadata["limit_applied"] = int(row_limit)
        elif pagination_mode == "offset" and page_size is not None:
            tenant_enforcement_metadata["limit_applied"] = int(page_size)

        partial_reason = last_truncated_reason
        if partial_reason is None and size_truncated:
            partial_reason = size_truncated_reason or PayloadTruncationReason.MAX_BYTES.value
        if partial_reason is None and forced_limited:
            partial_reason = PayloadTruncationReason.PROVIDER_CAP.value
        if partial_reason is None and row_limit_truncated:
            partial_reason = row_limit_result.partial_reason
        if partial_reason is None and safety_truncated:
            partial_reason = PayloadTruncationReason.SAFETY_LIMIT.value
        if partial_reason is None and is_truncated:
            partial_reason = PayloadTruncationReason.MAX_ROWS.value
        partial_reason = _normalize_partial_reason(partial_reason)
        cap_detected = partial_reason == PayloadTruncationReason.PROVIDER_CAP.value
        cap_mitigation_applied = False
        cap_mitigation_mode = "none"
        if cap_detected and cap_mitigation_setting == "safe":
            if caps.supports_pagination:
                if next_token:
                    cap_mitigation_applied = True
                    cap_mitigation_mode = "pagination_continuation"
                else:
                    cap_mitigation_mode = "pagination_unavailable"
            else:
                cap_mitigation_applied = True
                cap_mitigation_mode = "limited_view"
                if row_limit <= 0:
                    row_limit = len(result_rows)

        envelope_metadata = ExecuteSQLQueryMetadata(
            rows_returned=len(result_rows),
            is_truncated=is_truncated,
            partial=is_truncated,
            provider=provider,
            is_paginated=bool(
                applied_page_size
                or page_token
                or keyset_cursor
                or next_token
                or tenant_enforcement_metadata.get("next_keyset_cursor")
            ),
            row_limit=int(row_limit or 0) if row_limit else None,
            next_page_token=next_token,
            page_size=applied_page_size,
            page_items_returned=len(result_rows),
            partial_reason=partial_reason,
            items_returned=len(result_rows),
            bytes_returned=bytes_returned,
            limit_applied=tenant_enforcement_metadata.get("limit_applied"),
            execution_duration_ms=execution_duration_ms,
            cap_detected=cap_detected,
            cap_mitigation_applied=cap_mitigation_applied,
            cap_mitigation_mode=cap_mitigation_mode,
            # Capability negotiation
            capability_required=capability_metadata.get("capability_required"),
            capability_supported=capability_metadata.get("capability_supported"),
            fallback_policy=capability_metadata.get("fallback_policy"),
            fallback_applied=capability_metadata.get("fallback_applied"),
            fallback_mode=capability_metadata.get("fallback_mode"),
            tenant_enforcement_applied=tenant_enforcement_metadata.get(
                "tenant_enforcement_applied"
            ),
            tenant_enforcement_mode=tenant_enforcement_metadata.get("tenant_enforcement_mode"),
            tenant_rewrite_outcome=tenant_enforcement_metadata.get("tenant_rewrite_outcome"),
            tenant_rewrite_reason_code=tenant_enforcement_metadata.get(
                "tenant_rewrite_reason_code"
            ),
            session_guardrail_applied=tenant_enforcement_metadata.get("session_guardrail_applied"),
            session_guardrail_outcome=tenant_enforcement_metadata.get("session_guardrail_outcome"),
            execution_role_applied=tenant_enforcement_metadata.get("execution_role_applied"),
            execution_role_name=tenant_enforcement_metadata.get("execution_role_name"),
            restricted_session_mode=tenant_enforcement_metadata.get("restricted_session_mode"),
            session_guardrail_capability_mismatch=tenant_enforcement_metadata.get(
                "session_guardrail_capability_mismatch"
            ),
            sandbox_applied=tenant_enforcement_metadata.get("sandbox_applied"),
            sandbox_outcome=tenant_enforcement_metadata.get("sandbox_outcome"),
            sandbox_rollback=tenant_enforcement_metadata.get("sandbox_rollback"),
            sandbox_failure_reason=tenant_enforcement_metadata.get("sandbox_failure_reason"),
            session_reset_attempted=tenant_enforcement_metadata.get("session_reset_attempted"),
            session_reset_outcome=tenant_enforcement_metadata.get("session_reset_outcome"),
            execution_timeout_applied=tenant_enforcement_metadata.get("execution_timeout_applied"),
            execution_timeout_triggered=tenant_enforcement_metadata.get(
                "execution_timeout_triggered"
            ),
            resource_capability_mismatch=tenant_enforcement_metadata.get(
                "resource_capability_mismatch"
            ),
            pagination_mode_requested=tenant_enforcement_metadata.get("pagination_mode_requested"),
            pagination_mode_used=tenant_enforcement_metadata.get("pagination_mode_used"),
            next_keyset_cursor=tenant_enforcement_metadata.get("next_keyset_cursor"),
            **{
                "pagination.keyset.partial_page": tenant_enforcement_metadata.get(
                    "pagination.keyset.partial_page"
                ),
                "pagination.keyset.effective_page_size": tenant_enforcement_metadata.get(
                    "pagination.keyset.effective_page_size"
                ),
                "pagination.keyset.adaptive_page_size": tenant_enforcement_metadata.get(
                    "pagination.keyset.adaptive_page_size"
                ),
                "pagination.keyset.byte_budget": tenant_enforcement_metadata.get(
                    "pagination.keyset.byte_budget"
                ),
                "pagination.keyset.schema_used": tenant_enforcement_metadata.get(
                    "pagination.keyset.schema_used"
                ),
                "pagination.keyset.schema_strict": tenant_enforcement_metadata.get(
                    "pagination.keyset.schema_strict"
                ),
                "pagination.keyset.schema_stale": tenant_enforcement_metadata.get(
                    "pagination.keyset.schema_stale"
                ),
                "pagination.keyset.snapshot_strict": tenant_enforcement_metadata.get(
                    "pagination.keyset.snapshot_strict"
                ),
                "pagination.keyset.snapshot_id_present": tenant_enforcement_metadata.get(
                    "pagination.keyset.snapshot_id_present"
                ),
                "pagination.keyset.snapshot_mismatch": tenant_enforcement_metadata.get(
                    "pagination.keyset.snapshot_mismatch"
                ),
                "pagination.keyset.db_role": tenant_enforcement_metadata.get(
                    "pagination.keyset.db_role"
                ),
                "pagination.keyset.region": tenant_enforcement_metadata.get(
                    "pagination.keyset.region"
                ),
                "pagination.keyset.node_id_present": tenant_enforcement_metadata.get(
                    "pagination.keyset.node_id_present"
                ),
                "pagination.keyset.topology_mismatch": tenant_enforcement_metadata.get(
                    "pagination.keyset.topology_mismatch"
                ),
                "pagination.keyset.topology_available": tenant_enforcement_metadata.get(
                    "pagination.keyset.topology_available"
                ),
                "pagination.keyset.topology_strict": tenant_enforcement_metadata.get(
                    "pagination.keyset.topology_strict"
                ),
                "pagination.keyset.shard_id": tenant_enforcement_metadata.get(
                    "pagination.keyset.shard_id"
                ),
                "pagination.keyset.shard_key_hash_present": tenant_enforcement_metadata.get(
                    "pagination.keyset.shard_key_hash_present"
                ),
                "pagination.keyset.shard_mismatch": tenant_enforcement_metadata.get(
                    "pagination.keyset.shard_mismatch"
                ),
                "pagination.keyset.shard_info_available": tenant_enforcement_metadata.get(
                    "pagination.keyset.shard_info_available"
                ),
                "pagination.keyset.cross_shard_mode": tenant_enforcement_metadata.get(
                    "pagination.keyset.cross_shard_mode"
                ),
                "pagination.keyset.partition_signature": tenant_enforcement_metadata.get(
                    "pagination.keyset.partition_signature"
                ),
                "pagination.keyset.partition_signature_available": tenant_enforcement_metadata.get(
                    "pagination.keyset.partition_signature_available"
                ),
                "pagination.keyset.partition_set_changed": tenant_enforcement_metadata.get(
                    "pagination.keyset.partition_set_changed"
                ),
                "pagination.keyset.replica_lag_seconds": tenant_enforcement_metadata.get(
                    "pagination.keyset.replica_lag_seconds"
                ),
                "pagination.keyset.isolation_level": tenant_enforcement_metadata.get(
                    "pagination.keyset.isolation_level"
                ),
                "pagination.keyset.isolation_enforced": tenant_enforcement_metadata.get(
                    "pagination.keyset.isolation_enforced"
                ),
                "pagination.keyset.rejection_reason_code": tenant_enforcement_metadata.get(
                    "pagination.keyset.rejection_reason_code"
                ),
                "pagination.keyset.page_size_effective": tenant_enforcement_metadata.get(
                    "pagination.keyset.page_size_effective"
                ),
                "pagination.keyset.cursor_emitted": tenant_enforcement_metadata.get(
                    "pagination.keyset.cursor_emitted"
                ),
                "pagination.keyset.streaming_terminated": tenant_enforcement_metadata.get(
                    "pagination.keyset.streaming_terminated"
                ),
                "pagination.execution_topology": tenant_enforcement_metadata.get(
                    "pagination.execution_topology"
                ),
                "pagination.federated.ordering_supported": tenant_enforcement_metadata.get(
                    "pagination.federated.ordering_supported"
                ),
                "pagination.backend_set_sig_present": tenant_enforcement_metadata.get(
                    "pagination.backend_set_sig_present"
                ),
                "pagination.backend_set_mismatch": tenant_enforcement_metadata.get(
                    "pagination.backend_set_mismatch"
                ),
                "pagination.reject_reason_code": tenant_enforcement_metadata.get(
                    "pagination.reject_reason_code"
                ),
                "cursor_issued_at_present": tenant_enforcement_metadata.get(
                    "cursor_issued_at_present"
                ),
                "cursor_age_bucket": tenant_enforcement_metadata.get("cursor_age_bucket"),
                "cursor_validation_outcome": tenant_enforcement_metadata.get(
                    "cursor_validation_outcome"
                ),
                "pagination.cursor.signing_secret_configured": tenant_enforcement_metadata.get(
                    "pagination.cursor.signing_secret_configured"
                ),
                "pagination.cursor.signature_valid": tenant_enforcement_metadata.get(
                    "pagination.cursor.signature_valid"
                ),
                "pagination.cursor.legacy_issued_at_accepted": tenant_enforcement_metadata.get(
                    "pagination.cursor.legacy_issued_at_accepted"
                ),
                "pagination.cursor.issued_at_present": tenant_enforcement_metadata.get(
                    "pagination.cursor.issued_at_present"
                ),
            },
        )
        # print(f"DEBUG: metadata={envelope_metadata}")

        envelope = ExecuteSQLQueryResponseEnvelope(
            rows=result_rows, columns=columns, metadata=envelope_metadata
        )
        _record_tenant_enforcement_observability(tenant_enforcement_metadata)
        _record_session_guardrail_observability(tenant_enforcement_metadata)
        _record_sandbox_observability(tenant_enforcement_metadata)
        _record_timeout_observability(tenant_enforcement_metadata)
        _record_keyset_schema_observability(tenant_enforcement_metadata)
        _record_result_contract_observability(
            partial=is_truncated,
            partial_reason=partial_reason,
            items_returned=len(result_rows),
            page_size=applied_page_size,
            page_items_returned=len(result_rows),
            next_page_token=next_token,
            bytes_returned=bytes_returned,
            execution_duration_ms=execution_duration_ms,
            pagination_mode_requested=tenant_enforcement_metadata.get("pagination_mode_requested"),
            pagination_mode_used=tenant_enforcement_metadata.get("pagination_mode_used"),
            keyset_partial_page=tenant_enforcement_metadata.get("pagination.keyset.partial_page"),
            keyset_streaming_terminated=tenant_enforcement_metadata.get(
                "pagination.keyset.streaming_terminated"
            ),
            keyset_effective_page_size=tenant_enforcement_metadata.get(
                "pagination.keyset.effective_page_size"
            ),
            keyset_adaptive_page_size=tenant_enforcement_metadata.get(
                "pagination.keyset.adaptive_page_size"
            ),
            keyset_byte_budget=tenant_enforcement_metadata.get("pagination.keyset.byte_budget"),
            keyset_cursor_emitted=tenant_enforcement_metadata.get(
                "pagination.keyset.cursor_emitted"
            ),
            next_keyset_cursor=tenant_enforcement_metadata.get("next_keyset_cursor"),
            execution_topology=tenant_enforcement_metadata.get("pagination.execution_topology"),
            federated_ordering_supported=tenant_enforcement_metadata.get(
                "pagination.federated.ordering_supported"
            ),
            backend_set_sig_present=tenant_enforcement_metadata.get(
                "pagination.backend_set_sig_present"
            ),
            backend_set_mismatch=tenant_enforcement_metadata.get("pagination.backend_set_mismatch"),
            pagination_reject_reason_code=tenant_enforcement_metadata.get(
                "pagination.reject_reason_code"
            ),
        )

        return envelope.model_dump_json(exclude_none=True, by_alias=True)

    except _SandboxExecutionTimeout as e:
        provider = _active_provider()
        tenant_enforcement_metadata.update(_extract_postgres_sandbox_metadata(e))
        tenant_enforcement_metadata["execution_timeout_triggered"] = True
        if tenant_enforcement_metadata.get("pagination_mode_used") == "keyset":
            tenant_enforcement_metadata["pagination.keyset.partial_page"] = True
            tenant_enforcement_metadata["pagination.keyset.cursor_emitted"] = False
            tenant_enforcement_metadata["next_keyset_cursor"] = None
            if streaming:
                tenant_enforcement_metadata["pagination.keyset.streaming_terminated"] = True
        tenant_enforcement_metadata["partial_reason"] = "timeout"
        return _construct_error_response(
            execution_started_at,
            message="Execution timed out.",
            category=ErrorCategory.TIMEOUT,
            provider=provider,
            envelope_metadata=tenant_enforcement_metadata,
        )
    except OffsetPaginationTokenError as e:
        provider = _active_provider()
        _apply_cursor_decode_metadata(
            tenant_enforcement_metadata,
            offset_decode_metadata,
            fallback_reason_code=e.reason_code,
        )
        return _construct_error_response(
            execution_started_at,
            message=str(e),
            category=ErrorCategory.INVALID_REQUEST,
            provider=provider,
            metadata={"reason_code": e.reason_code},
            envelope_metadata=tenant_enforcement_metadata,
        )
    except SessionGuardrailPolicyError as e:
        provider = _active_provider()
        session_guardrail_metadata = dict(getattr(e, "envelope_metadata", {}) or {})
        tenant_enforcement_metadata.update(session_guardrail_metadata)
        tenant_enforcement_metadata.update(_extract_postgres_sandbox_metadata(e))
        return _construct_error_response(
            execution_started_at,
            message=str(e),
            category=ErrorCategory.UNSUPPORTED_CAPABILITY,
            provider=provider,
            metadata={
                "reason_code": e.reason_code,
                "session_guardrail_outcome": e.outcome,
            },
            envelope_metadata=tenant_enforcement_metadata,
        )
    except asyncpg.PostgresError as e:
        provider = _active_provider()
        tenant_enforcement_metadata.update(_extract_postgres_sandbox_metadata(e))
        metadata = extract_error_metadata(provider, e)
        emit_classified_error(provider, "execute_sql_query", metadata.category, e)
        return _construct_error_response(
            execution_started_at,
            message=metadata.message,
            category=metadata.category,
            provider=provider,
            is_retryable=metadata.is_retryable,
            retry_after_seconds=metadata.retry_after_seconds,
            metadata=metadata.to_dict(),  # include raw details if any
            envelope_metadata=tenant_enforcement_metadata,
        )
    except Exception as e:
        provider = _active_provider()
        tenant_enforcement_metadata.update(_extract_postgres_sandbox_metadata(e))
        metadata = extract_error_metadata(provider, e)
        emit_classified_error(provider, "execute_sql_query", metadata.category, e)
        return _construct_error_response(
            execution_started_at,
            message=metadata.message,
            category=metadata.category,
            provider=provider,
            is_retryable=metadata.is_retryable,
            retry_after_seconds=metadata.retry_after_seconds,
            metadata=metadata.to_dict(),
            envelope_metadata=tenant_enforcement_metadata,
        )
