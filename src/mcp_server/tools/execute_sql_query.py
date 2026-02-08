"""MCP tool: execute_sql_query - Execute read-only SQL queries."""

import asyncio
import json
from typing import Any, Dict, Optional

import asyncpg

from common.config.env import get_env_bool, get_env_int, get_env_str
from common.constants.reason_codes import PayloadTruncationReason
from common.models.error_metadata import ErrorMetadata
from common.models.tool_envelopes import ExecuteSQLQueryMetadata, ExecuteSQLQueryResponseEnvelope
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

TOOL_NAME = "execute_sql_query"


def _build_columns_from_rows(rows: list[dict]) -> list[dict]:
    if not rows:
        return []
    first_row = rows[0]
    return [build_column_meta(key, "unknown") for key in first_row.keys()]


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
    category: str = "unknown",
    metadata: Optional[Dict[str, Any]] = None,
    provider: str = "unknown",
    is_retryable: bool = False,
    retry_after_seconds: Optional[float] = None,
) -> str:
    """Construct a standardized error response."""
    legacy_mode = get_env_bool("MCP_EXECUTE_SQL_LEGACY_OUTPUT", False)

    if legacy_mode:
        payload = {
            "error": message,
            "error_category": category,
        }
        if metadata:
            payload.update(metadata)
        if retry_after_seconds is not None:
            payload["retry_after_seconds"] = retry_after_seconds
        return json.dumps(payload, separators=(",", ":"))

    # Envelope Mode
    meta_dict = (metadata or {}).copy()
    # Remove keys that are passed explicitly to avoid multiple values error
    for key in ["message", "category", "provider", "is_retryable", "retry_after_seconds"]:
        meta_dict.pop(key, None)

    error_meta = ErrorMetadata(
        message=message,
        category=category,
        provider=provider,
        is_retryable=is_retryable,
        retry_after_seconds=retry_after_seconds,
        **meta_dict,
    )

    envelope = ExecuteSQLQueryResponseEnvelope(
        rows=[],
        metadata=ExecuteSQLQueryMetadata(
            rows_returned=0,
            is_truncated=False,
        ),
        error=error_meta,
    )
    return envelope.model_dump_json(exclude_none=True)


def _validate_sql_ast(sql: str, provider: str) -> Optional[str]:
    """Validate SQL AST using sqlglot to ensure single-statement SELECT only."""
    import sqlglot

    # Map Text2SQL provider names to sqlglot dialects
    dialect_map = {
        "postgres": "postgres",
        "postgresql": "postgres",
        "redshift": "redshift",
        "sqlite": "sqlite",
        "duckdb": "duckdb",
        "mysql": "mysql",
        "bigquery": "bigquery",
        "snowflake": "snowflake",
    }
    dialect = dialect_map.get(provider.lower(), "postgres")

    try:
        expressions = sqlglot.parse(sql, read=dialect)
        if not expressions:
            return "Empty or invalid SQL query."

        if len(expressions) > 1:
            return "Multi-statement queries are forbidden."

        expression = expressions[0]
        if expression is None:
            return "Failed to parse SQL query."

        # Ensure it's a SELECT statement (including WITH clauses and UNIONS)
        # Using .key check as it's more stable across sqlglot versions than class checks
        # sqlglot.exp.Query covers Select, Union, Intersect, Except
        if expression.key not in ("select", "union", "intersect", "except", "with"):
            return f"Forbidden statement type: {expression.key.upper()}. Only SELECT is allowed."

    except sqlglot.errors.ParseError as e:
        return f"SQL Syntax Error: {e}"
    except Exception as e:
        return f"SQL Validation Error: {str(e)}"

    return None


async def handler(
    sql_query: str,
    tenant_id: Optional[int] = None,
    params: Optional[list] = None,
    include_columns: bool = False,
    timeout_seconds: Optional[float] = None,
    page_token: Optional[str] = None,
    page_size: Optional[int] = None,
) -> str:
    """Execute a valid SQL SELECT statement and return the result as JSON."""
    provider = Database.get_query_target_provider()

    # 1. Server-Side AST Validation
    validation_error = _validate_sql_ast(sql_query, provider)
    if validation_error:
        return _construct_error_response(
            validation_error,
            category="invalid_request",
            provider=provider,
        )

    # 2. Authorization Check (Tenant Context)
    if tenant_id is None:
        return _construct_error_response(
            "Unauthorized. No Tenant ID context found. "
            "Set X-Tenant-ID header or DEFAULT_TENANT_ID env var.",
            category="unsupported_capability",
            provider=provider,
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

        return _construct_error_response(
            message=f"Requested capability is not supported: {required_capability}.",
            category="unsupported_capability",
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

    caps = Database.get_query_target_capabilities()
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
                category="invalid_request",
                provider=provider,
            )
        if page_size > max_page_size:
            page_size = max_page_size

    if provider == "redshift":
        from dal.redshift import validate_redshift_query

        errors = validate_redshift_query(sql_query)
        if errors:
            return _construct_error_response(
                "Redshift query validation failed.",
                category="invalid_request",
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
                            sql_query, page_token, effective_page_size, *(params or [])
                        )
                        return rows
                    rows, next_token = await fetch_page(
                        sql_query, page_token, effective_page_size, *(params or [])
                    )
                    return rows
                if include_columns:
                    fetch_with_columns = getattr(conn, "fetch_with_columns", None)
                    prepare = getattr(conn, "prepare", None)
                    supports_fetch_with_columns = (
                        callable(fetch_with_columns) and "fetch_with_columns" in type(conn).__dict__
                    )
                    supports_prepare = callable(prepare) and "prepare" in type(conn).__dict__
                    if params:
                        if supports_fetch_with_columns:
                            rows, columns = await fetch_with_columns(sql_query, *params)
                        elif supports_prepare:
                            from dal.util.column_metadata import columns_from_asyncpg_attributes

                            statement = await prepare(sql_query)
                            rows = await statement.fetch(*params)
                            columns = columns_from_asyncpg_attributes(statement.get_attributes())
                            rows = [dict(row) for row in rows]
                        else:
                            rows = await conn.fetch(sql_query, *params)
                            rows = [dict(row) for row in rows]
                    else:
                        if supports_fetch_with_columns:
                            rows, columns = await fetch_with_columns(sql_query)
                        elif supports_prepare:
                            from dal.util.column_metadata import columns_from_asyncpg_attributes

                            statement = await prepare(sql_query)
                            rows = await statement.fetch()
                            columns = columns_from_asyncpg_attributes(statement.get_attributes())
                            rows = [dict(row) for row in rows]
                        else:
                            rows = await conn.fetch(sql_query)
                            rows = [dict(row) for row in rows]
                else:
                    if params:
                        rows = await conn.fetch(sql_query, *params)
                    else:
                        rows = await conn.fetch(sql_query)
                    rows = [dict(row) for row in rows]
                return rows

            try:
                result_rows = await run_with_timeout(
                    _fetch_rows, timeout_seconds, cancel=lambda: _cancel_best_effort(conn)
                )
            except asyncio.TimeoutError:
                return _construct_error_response(
                    "Execution timed out.", category="timeout", provider=provider
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

        legacy_mode = get_env_bool("MCP_EXECUTE_SQL_LEGACY_OUTPUT", False)
        if legacy_mode:
            payload = {
                "rows": result_rows,
                "metadata": {
                    "is_truncated": is_truncated,
                    "row_limit": int(row_limit or 0),
                    "rows_returned": len(result_rows),
                    "next_page_token": next_token,
                    "page_size": effective_page_size,
                    "partial_reason": partial_reason,
                    "cap_detected": cap_detected,
                    "cap_mitigation_applied": cap_mitigation_applied,
                    "cap_mitigation_mode": cap_mitigation_mode,
                    **capability_metadata,
                },
            }
            if include_columns:
                payload["columns"] = columns
            return json.dumps(payload, default=str, separators=(",", ":"))

        # Typed Envelope Construction
        envelope_metadata = ExecuteSQLQueryMetadata(
            rows_returned=len(result_rows),
            is_truncated=is_truncated,
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
        provider = Database.get_query_target_provider()
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
        provider = Database.get_query_target_provider()
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
