"""MCP tool: execute_sql_query - Execute read-only SQL queries."""

import asyncio
import json
import logging
from typing import Any, Dict, List, Optional, Sequence

import asyncpg
from opentelemetry import trace

from agent.audit import AuditEventSource, AuditEventType, emit_audit_event
from common.config.env import get_env_int, get_env_str
from common.constants.reason_codes import PayloadTruncationReason
from common.errors.error_codes import ErrorCode
from common.models.error_metadata import ErrorCategory
from common.models.tool_envelopes import ExecuteSQLQueryMetadata, ExecuteSQLQueryResponseEnvelope
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
from dal.util.column_metadata import build_column_meta
from dal.util.row_limits import get_sync_max_rows
from dal.util.timeouts import run_with_timeout
from mcp_server.utils.json_budget import JSONBudget
from mcp_server.utils.provider import resolve_provider

TOOL_NAME = "execute_sql_query"
TOOL_DESCRIPTION = "Execute a validated SQL query against the target database."
logger = logging.getLogger(__name__)

_TENANT_REWRITE_REASON_ALLOWLIST = {
    "COMPLETENESS_FAILED",
    "CORRELATED_SUBQUERY_UNSUPPORTED",
    "CTE_UNSUPPORTED_SHAPE",
    "MISSING_TENANT_COLUMN",
    "MISSING_TENANT_COLUMN_CONFIG",
    "NESTED_FROM_UNSUPPORTED",
    "NO_PREDICATES_PRODUCED",
    "NOT_SELECT_STATEMENT",
    "NOT_SINGLE_SELECT",
    "PARAM_LIMIT_EXCEEDED",
    "PARSE_ERROR",
    "PROVIDER_UNSUPPORTED",
    "REWRITE_DISABLED",
    "SET_OPERATIONS_UNSUPPORTED",
    "SUBQUERY_UNSUPPORTED",
    "TARGET_LIMIT_EXCEEDED",
    "UNRESOLVABLE_TABLE_ALIAS",
    "UNRESOLVABLE_TABLE_IDENTITY",
    "WINDOW_FUNCTIONS_UNSUPPORTED",
}
_TENANT_REWRITE_INTERNAL_REASON_ALLOWLIST = {
    "SCHEMA_METADATA_MISSING",
    "SCHEMA_TENANT_COLUMN_MISSING",
    "TENANT_MODE_UNSUPPORTED",
}


def _bounded_tenant_rewrite_reason_code(reason_code: str | None) -> str:
    """Return a low-cardinality tenant rewrite reason code suitable for diagnostics."""
    normalized = (reason_code or "").strip().upper()
    if normalized in _TENANT_REWRITE_REASON_ALLOWLIST:
        return f"tenant_rewrite_{normalized.lower()}"
    if normalized in _TENANT_REWRITE_INTERNAL_REASON_ALLOWLIST:
        return f"tenant_rewrite_{normalized.lower()}"
    return "tenant_enforcement_unsupported"


def _record_tenant_rewrite_failure_reason(reason_code: str | None) -> None:
    """Attach bounded tenant rewrite failure diagnostics to the active span."""
    span = trace.get_current_span()
    if span is None or not span.is_recording():
        return
    span.set_attribute("tenant_rewrite.failure_reason", (reason_code or "UNKNOWN").strip().upper())
    span.set_attribute(
        "tenant_rewrite.failure_reason_code",
        _bounded_tenant_rewrite_reason_code(reason_code),
    )


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


def _tenant_enforcement_unsupported_response(provider: str, reason_code: str | None = None) -> str:
    """Return a canonical tenant-enforcement unsupported response envelope."""
    return _construct_error_response(
        message="Tenant enforcement not supported for provider/table configuration.",
        category=ErrorCategory.TENANT_ENFORCEMENT_UNSUPPORTED,
        provider=provider,
        metadata={
            "sql_state": "TENANT_ENFORCEMENT_UNSUPPORTED",
            "error_code": ErrorCode.TENANT_ENFORCEMENT_UNSUPPORTED.value,
            "reason_code": _bounded_tenant_rewrite_reason_code(reason_code),
        },
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


def _tenant_scope_schema_validation_failures(
    table_names: Sequence[str],
    table_columns: dict[str, set[str]],
    tenant_column: str,
) -> tuple[list[str], list[str]]:
    """Return tables missing schema metadata and missing tenant column."""
    tenant_column_normalized = tenant_column.strip().lower()
    if not tenant_column_normalized:
        return [], []

    missing_schema: list[str] = []
    missing_tenant_column: list[str] = []
    for table_name in table_names:
        normalized = (table_name or "").strip().lower()
        if not normalized:
            continue
        columns = table_columns.get(normalized)
        if columns is None:
            columns = table_columns.get(normalized.split(".")[-1])
        if columns is None:
            missing_schema.append(normalized)
            continue
        if tenant_column_normalized not in columns:
            missing_tenant_column.append(normalized)
    return missing_schema, missing_tenant_column


def _resolve_row_limit(conn: object) -> int:
    max_rows = getattr(conn, "max_rows", None)
    if not max_rows:
        max_rows = getattr(conn, "_max_rows", None)
    if not max_rows:
        max_rows = get_sync_max_rows()
    return int(max_rows or 0)


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

    envelope = ExecuteSQLQueryResponseEnvelope(
        rows=[],
        metadata=ExecuteSQLQueryMetadata(
            rows_returned=0,
            is_truncated=False,
            provider=resolved_provider,
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


def _validate_sql_ast(sql: str, provider: str) -> Optional[str]:
    """Validate SQL AST using sqlglot to ensure single-statement SELECT only."""
    import sqlglot

    from common.sql.comments import strip_sql_comments

    # Map Text2SQL provider names to sqlglot dialects
    dialect = normalize_sqlglot_dialect(provider)
    stripped_sql = strip_sql_comments(sql)

    try:
        expressions = sqlglot.parse(stripped_sql, read=dialect)
        if not expressions:
            return "Empty or invalid SQL query."

        if len(expressions) > 1:
            return "Multi-statement queries are forbidden."

        expression = expressions[0]
        if expression is None:
            return "Failed to parse SQL query."

        # Use centralized policy
        from common.policy.sql_policy import (
            ALLOWED_STATEMENT_TYPES,
            BLOCKED_FUNCTIONS,
            classify_blocked_table_reference,
        )

        if expression.key not in ALLOWED_STATEMENT_TYPES:
            allowed_list = ", ".join(sorted([t.upper() for t in ALLOWED_STATEMENT_TYPES]))
            return (
                f"Forbidden statement type: {expression.key.upper()}. "
                f"Only {allowed_list} are allowed."
            )

        # Block dangerous functions
        import sqlglot.expressions as exp

        for node in expression.find_all(exp.Anonymous):
            if str(node.this).lower() in BLOCKED_FUNCTIONS:
                return f"Forbidden function: {str(node.this).upper()} is not allowed."

        for node in expression.find_all(exp.Func):
            func_name = node.sql_name().lower()
            if func_name in BLOCKED_FUNCTIONS:
                return f"Forbidden function: {func_name.upper()} is not allowed."

        # Block restricted/system tables and schemas for direct MCP invocations.
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
                return f"Forbidden table: {full_name} is not allowed."
            return f"Forbidden schema/table reference: {full_name} is not allowed."

    except sqlglot.errors.ParseError as e:
        return f"SQL Syntax Error: {e}"
    except Exception:
        return "SQL Validation Error."

    return None


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


async def handler(
    sql_query: str,
    tenant_id: int,
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

    from mcp_server.utils.auth import validate_role

    if err := validate_role("SQL_ADMIN_ROLE", TOOL_NAME):
        return err

    # 5. Execute Query
    from mcp_server.utils.validation import require_tenant_id

    # 0. Enforce Tenant ID
    if err := require_tenant_id(tenant_id, TOOL_NAME):
        return err

    caps = Database.get_query_target_capabilities()
    tenant_mode_raw = getattr(caps, "tenant_enforcement_mode", None)
    if isinstance(tenant_mode_raw, str):
        tenant_enforcement_mode = tenant_mode_raw.strip().lower() or "rls_session"
    else:
        tenant_enforcement_mode = "rls_session"

    if tenant_id is not None and tenant_enforcement_mode == "unsupported":
        _record_tenant_rewrite_failure_reason("TENANT_MODE_UNSUPPORTED")
        return _tenant_enforcement_unsupported_response(
            provider,
            reason_code="TENANT_MODE_UNSUPPORTED",
        )

    effective_sql_query = sql_query
    effective_params = list(params or [])

    # 1. SQL Length Check
    max_sql_len = get_env_int("MCP_MAX_SQL_LENGTH", 100 * 1024)
    if len(sql_query) > max_sql_len:
        return _construct_error_response(
            message=f"SQL query exceeds maximum length of {max_sql_len} bytes.",
            category=ErrorCategory.INVALID_REQUEST,
            provider=provider,
        )

    # 2. Server-Side AST Validation
    validation_error = _validate_sql_ast(sql_query, provider)
    if validation_error:
        if "forbidden statement type" in validation_error.lower():
            emit_audit_event(
                AuditEventType.READONLY_VIOLATION,
                source=AuditEventSource.MCP,
                tenant_id=tenant_id,
                error_category=ErrorCategory.INVALID_REQUEST,
                metadata={
                    "provider": provider,
                    "reason_code": "ast_forbidden_statement_type",
                    "decision": "reject",
                },
            )
        return _construct_error_response(
            validation_error,
            category=ErrorCategory.INVALID_REQUEST,
            provider=provider,
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
        )

    # 1.5 Parameter Validation
    param_error = _validate_params(params)
    if param_error:
        return _construct_error_response(
            param_error,
            category=ErrorCategory.INVALID_REQUEST,
            provider=provider,
        )

    if tenant_id is not None and tenant_enforcement_mode == "sql_rewrite":
        from common.sql.tenant_sql_rewriter import TenantSQLRewriteError, rewrite_tenant_scoped_sql

        tenant_column = _tenant_column_name()
        global_table_allowlist = _tenant_global_table_allowlist()
        try:
            rewrite_result = rewrite_tenant_scoped_sql(
                effective_sql_query,
                provider=provider,
                tenant_id=tenant_id,
                tenant_column=tenant_column,
                global_table_allowlist=global_table_allowlist,
            )
        except TenantSQLRewriteError as e:
            _record_tenant_rewrite_failure_reason(e.reason_code)
            return _tenant_enforcement_unsupported_response(provider, reason_code=e.reason_code)

        table_columns = await _load_table_columns_for_rewrite(
            rewrite_result.tables_rewritten,
            tenant_id,
        )
        missing_schema_tables, missing_tenant_column_tables = (
            _tenant_scope_schema_validation_failures(
                rewrite_result.tables_rewritten,
                table_columns,
                tenant_column,
            )
        )
        if missing_schema_tables or missing_tenant_column_tables:
            schema_reason_code = (
                "SCHEMA_TENANT_COLUMN_MISSING"
                if missing_tenant_column_tables
                else "SCHEMA_METADATA_MISSING"
            )
            _record_tenant_rewrite_failure_reason(schema_reason_code)
            return _tenant_enforcement_unsupported_response(
                provider,
                reason_code=schema_reason_code,
            )

        effective_sql_query = rewrite_result.rewritten_sql
        effective_params.extend(rewrite_result.params)

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

    max_page_size = 1000
    if page_size is not None:
        if page_size <= 0:
            return _construct_error_response(
                "Invalid page_size: must be greater than zero.",
                category=ErrorCategory.INVALID_REQUEST,
                provider=provider,
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
            )

    try:
        columns = None
        last_truncated = False
        row_limit = 0
        next_token = None
        async with Database.get_connection(tenant_id, read_only=True) as conn:
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
                    _fetch_rows, timeout_seconds, cancel=lambda: _cancel_best_effort(conn)
                )
            except asyncio.TimeoutError:
                return _construct_error_response(
                    "Execution timed out.", category=ErrorCategory.TIMEOUT, provider=provider
                )

            raw_last_truncated = getattr(conn, "last_truncated", False)
            last_truncated = raw_last_truncated if isinstance(raw_last_truncated, bool) else False
            raw_reason = getattr(conn, "last_truncated_reason", None)
            last_truncated_reason = raw_reason if isinstance(raw_reason, str) else None

        # Size Safety Valve
        safety_limit = 1000
        safety_truncated = False
        if len(result_rows) > safety_limit:
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

        # JSON Size Budget
        max_bytes = get_env_int("MCP_JSON_PAYLOAD_LIMIT_BYTES", 2 * 1024 * 1024)
        budget = JSONBudget(max_bytes)
        safe_rows = []
        size_truncated = False

        # Approximate envelope overhead
        # In new envelope, we have additional overhead from ErrorMetadata structure
        # if present (none here) but also Metadata fields.
        budget.consume({"metadata": {}, "rows": []})

        for row in result_rows:
            if not budget.consume(row):
                size_truncated = True
                break
            safe_rows.append(row)

        result_rows = safe_rows

        if include_columns and not columns:
            columns = _build_columns_from_rows(result_rows)

        is_truncated = bool(last_truncated or safety_truncated or forced_limited or size_truncated)
        partial_reason = last_truncated_reason
        if partial_reason is None and size_truncated:
            partial_reason = PayloadTruncationReason.MAX_BYTES.value
        if partial_reason is None and forced_limited:
            partial_reason = PayloadTruncationReason.PROVIDER_CAP.value
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
            cap_detected=cap_detected,
            cap_mitigation_applied=cap_mitigation_applied,
            cap_mitigation_mode=cap_mitigation_mode,
            # Capability negotiation
            capability_required=capability_metadata.get("capability_required"),
            capability_supported=capability_metadata.get("capability_supported"),
            fallback_policy=capability_metadata.get("fallback_policy"),
            fallback_applied=capability_metadata.get("fallback_applied"),
            fallback_mode=capability_metadata.get("fallback_mode"),
        )

        envelope = ExecuteSQLQueryResponseEnvelope(
            rows=result_rows, columns=columns, metadata=envelope_metadata
        )

        return envelope.model_dump_json(exclude_none=True)

    except asyncpg.PostgresError as e:
        provider = _active_provider()
        metadata = extract_error_metadata(provider, e)
        emit_classified_error(provider, "execute_sql_query", metadata.category, e)
        return _construct_error_response(
            message=metadata.message,
            category=metadata.category,
            provider=provider,
            is_retryable=metadata.is_retryable,
            retry_after_seconds=metadata.retry_after_seconds,
            metadata=metadata.to_dict(),  # include raw details if any
        )
    except Exception as e:
        provider = _active_provider()
        metadata = extract_error_metadata(provider, e)
        emit_classified_error(provider, "execute_sql_query", metadata.category, e)
        return _construct_error_response(
            message=metadata.message,
            category=metadata.category,
            provider=provider,
            is_retryable=metadata.is_retryable,
            retry_after_seconds=metadata.retry_after_seconds,
            metadata=metadata.to_dict(),
        )
