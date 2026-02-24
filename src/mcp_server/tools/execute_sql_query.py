"""MCP tool: execute_sql_query - Execute read-only SQL queries."""

import asyncio
import json
import logging
from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any, Dict, List, Optional, Sequence

import asyncpg
from opentelemetry import trace

from agent.audit import AuditEventSource, AuditEventType, emit_audit_event
from common.config.env import get_env_int, get_env_str
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
from dal.postgres_sandbox import (
    SANDBOX_FAILURE_NONE,
    SANDBOX_FAILURE_REASON_ALLOWLIST,
    build_postgres_sandbox_metadata,
)
from dal.resource_containment import enforce_byte_limit, enforce_row_limit
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


@dataclass(frozen=True)
class SQLASTValidationFailure:
    """Structured AST validation failure with stable classification fields."""

    message: str
    reason_code: str
    category: ErrorCategory = ErrorCategory.INVALID_REQUEST
    error_code: str = ErrorCode.VALIDATION_ERROR.value


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
    if details_safe:
        error_meta = error_meta.model_copy(update={"details_safe": details_safe})

    _record_tenant_enforcement_observability(envelope_metadata)
    _record_session_guardrail_observability(envelope_metadata)
    _record_sandbox_observability(envelope_metadata)
    _record_timeout_observability(envelope_metadata)

    envelope = ExecuteSQLQueryResponseEnvelope(
        rows=[],
        metadata=ExecuteSQLQueryMetadata(
            rows_returned=0,
            is_truncated=False,
            provider=resolved_provider,
            **(envelope_metadata or {}),
        ),
        error=error_meta,
    )
    return envelope.model_dump_json(exclude_none=True)


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
    try:
        resource_limits = ExecutionResourceLimits.from_env()
    except ValueError:
        return _construct_error_response(
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

    from mcp_server.utils.auth import validate_role

    if err := validate_role("SQL_ADMIN_ROLE", TOOL_NAME):
        return err

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
    tenant_enforcement_metadata = dict(policy_decision.envelope_metadata)
    tenant_enforcement_metadata.update(session_guardrail_metadata)
    tenant_enforcement_metadata.update(sandbox_metadata)
    tenant_enforcement_metadata["execution_timeout_applied"] = execution_timeout_applied
    tenant_enforcement_metadata["execution_timeout_triggered"] = False

    if tenant_id is not None and not policy_decision.should_execute:
        _record_policy_decision_telemetry(policy_decision.telemetry_attributes)
        return _tenant_enforcement_unsupported_response(
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
            message=f"SQL query exceeds maximum length of {max_sql_len} bytes.",
            category=ErrorCategory.INVALID_REQUEST,
            provider=provider,
            envelope_metadata=tenant_enforcement_metadata,
        )

    # 2. Server-Side AST Validation
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
            str(e),
            category=ErrorCategory.INVALID_REQUEST,
            provider=provider,
            envelope_metadata=tenant_enforcement_metadata,
        )

    # 1.5 Parameter Validation
    param_error = _validate_params(params)
    if param_error:
        return _construct_error_response(
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
    tenant_enforcement_metadata = dict(policy_decision.envelope_metadata)
    tenant_enforcement_metadata.update(session_guardrail_metadata)
    tenant_enforcement_metadata.update(sandbox_metadata)
    tenant_enforcement_metadata["execution_timeout_applied"] = execution_timeout_applied
    tenant_enforcement_metadata["execution_timeout_triggered"] = False
    effective_sql_query = policy_decision.sql_to_execute
    effective_params = list(policy_decision.params_to_bind)
    if not policy_decision.should_execute:
        return _tenant_enforcement_unsupported_response(
            provider,
            policy_decision=policy_decision,
            envelope_metadata=tenant_enforcement_metadata,
        )

    def _unsupported_capability_response(
        required_capability: str,
        provider_name: str,
        negotiation: Optional[CapabilityNegotiationResult] = None,
    ) -> str:
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
        bool(timeout_seconds and timeout_seconds > 0 and caps.execution_model == "async"),
        caps.supports_cancel,
    )
    if unsupported_response is not None:
        return unsupported_response
    unsupported_response = _negotiate_if_required(
        "pagination",
        bool(page_token or page_size),
        caps.supports_pagination,
    )
    if unsupported_response is not None:
        return unsupported_response

    max_page_size = (
        max(1, int(resource_limits.max_rows)) if resource_limits.enforce_row_limit else 1000
    )
    if page_size is not None:
        if page_size <= 0:
            return _construct_error_response(
                "Invalid page_size: must be greater than zero.",
                category=ErrorCategory.INVALID_REQUEST,
                provider=provider,
                envelope_metadata=tenant_enforcement_metadata,
            )
        if page_size > max_page_size:
            page_size = max_page_size

    if provider == "redshift":
        from dal.redshift import validate_redshift_query

        errors = validate_redshift_query(effective_sql_query)
        if errors:
            return _construct_error_response(
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
        next_token = None
        conn = None
        async with Database.get_connection(tenant_id=tenant_id, read_only=True) as conn:
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

            async def _fetch_rows():
                nonlocal columns, next_token
                fetch_page = getattr(conn, "fetch_page", None)
                fetch_page_with_columns = getattr(conn, "fetch_page_with_columns", None)
                if (page_token or effective_page_size) and callable(fetch_page):
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

        if conn is not None:
            tenant_enforcement_metadata.update(_extract_postgres_sandbox_metadata(conn))

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

        if include_columns and not columns:
            columns = _build_columns_from_rows(result_rows)

        is_truncated = bool(
            last_truncated
            or row_limit_truncated
            or safety_truncated
            or forced_limited
            or size_truncated
        )
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
        cap_detected = partial_reason == "PROVIDER_CAP"
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

        # Typed Envelope Construction (Legacy mode removed)
        envelope_metadata = ExecuteSQLQueryMetadata(
            rows_returned=len(result_rows),
            is_truncated=is_truncated,
            provider=provider,
            row_limit=int(row_limit or 0) if row_limit else None,
            next_page_token=next_token,
            partial_reason=partial_reason,
            bytes_returned=bytes_returned,
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
        )

        envelope = ExecuteSQLQueryResponseEnvelope(
            rows=result_rows, columns=columns, metadata=envelope_metadata
        )
        _record_tenant_enforcement_observability(tenant_enforcement_metadata)
        _record_session_guardrail_observability(tenant_enforcement_metadata)
        _record_sandbox_observability(tenant_enforcement_metadata)
        _record_timeout_observability(tenant_enforcement_metadata)

        return envelope.model_dump_json(exclude_none=True)

    except _SandboxExecutionTimeout as e:
        provider = _active_provider()
        tenant_enforcement_metadata.update(_extract_postgres_sandbox_metadata(e))
        tenant_enforcement_metadata["execution_timeout_triggered"] = True
        tenant_enforcement_metadata["partial_reason"] = "timeout"
        return _construct_error_response(
            message="Execution timed out.",
            category=ErrorCategory.TIMEOUT,
            provider=provider,
            envelope_metadata=tenant_enforcement_metadata,
        )
    except SessionGuardrailPolicyError as e:
        provider = _active_provider()
        session_guardrail_metadata = dict(getattr(e, "envelope_metadata", {}) or {})
        tenant_enforcement_metadata.update(session_guardrail_metadata)
        tenant_enforcement_metadata.update(_extract_postgres_sandbox_metadata(e))
        return _construct_error_response(
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
            message=metadata.message,
            category=metadata.category,
            provider=provider,
            is_retryable=metadata.is_retryable,
            retry_after_seconds=metadata.retry_after_seconds,
            metadata=metadata.to_dict(),
            envelope_metadata=tenant_enforcement_metadata,
        )
