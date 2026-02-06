"""MCP tool: execute_sql_query - Execute read-only SQL queries."""

import asyncio
import json
import re
from typing import Optional

import asyncpg

from common.config.env import get_env_str
from dal.capability_negotiation import (
    CapabilityNegotiationResult,
    negotiate_capability_request,
    parse_capability_fallback_policy,
)
from dal.database import Database
from dal.error_classification import emit_classified_error, maybe_classify_error
from dal.util.column_metadata import build_column_meta
from dal.util.row_limits import get_sync_max_rows
from dal.util.timeouts import run_with_timeout

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
            # Best effort cancellation, suppress errors but log if needed
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


async def handler(
    sql_query: str,
    tenant_id: Optional[int] = None,
    params: Optional[list] = None,
    include_columns: bool = False,
    timeout_seconds: Optional[float] = None,
    page_token: Optional[str] = None,
    page_size: Optional[int] = None,
) -> str:
    """Execute a valid SQL SELECT statement and return the result as JSON.

    Strictly read-only. Returns error messages as strings for self-correction.
    Requires tenant_id for RLS enforcement.

    Args:
        sql_query: A SQL SELECT query string.
        tenant_id: Required tenant identifier for RLS enforcement.
        params: Optional list of bind parameters (e.g. for rewritten queries).

    Returns:
        JSON array of result rows, or error message as string.
    """

    def _unsupported_capability_response(
        required_capability: str,
        provider_name: str,
        negotiation: Optional[CapabilityNegotiationResult] = None,
    ) -> str:
        capability_required = (
            negotiation.capability_required if negotiation else required_capability
        )
        capability_supported = negotiation.capability_supported if negotiation else False
        fallback_applied = negotiation.fallback_applied if negotiation else False
        fallback_mode = negotiation.fallback_mode if negotiation else "none"
        return json.dumps(
            {
                "error": f"Requested capability is not supported: {required_capability}.",
                "error_category": "unsupported_capability",
                "required_capability": required_capability,
                "capability_required": capability_required,
                "capability_supported": capability_supported,
                "fallback_applied": fallback_applied,
                "fallback_mode": fallback_mode,
                "provider": provider_name,
            },
            separators=(",", ":"),
        )

    # Require tenant_id for RLS enforcement
    if tenant_id is None:
        error_msg = (
            "Unauthorized. No Tenant ID context found. "
            "Set X-Tenant-ID header or DEFAULT_TENANT_ID env var."
        )
        return json.dumps({"error": error_msg})

    # Application-Level Security Check (Pre-flight)
    # Reject mutative keywords to provide immediate feedback.
    # Note: Real enforcement happens at the DB session/transaction level.
    forbidden_patterns = [
        r"(?i)\bDROP\b",
        r"(?i)\bDELETE\b",
        r"(?i)\bINSERT\b",
        r"(?i)\bUPDATE\b",
        r"(?i)\bALTER\b",
        r"(?i)\bGRANT\b",
        r"(?i)\bREVOKE\b",
        r"(?i)\bTRUNCATE\b",
        r"(?i)\bCREATE\b",
    ]

    for pattern in forbidden_patterns:
        if re.search(pattern, sql_query):
            return (
                f"Error: Query contains forbidden keyword matching '{pattern}'. "
                "Read-only access only."
            )

    provider = Database.get_query_target_provider()
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
            return json.dumps(
                {
                    "error": "Invalid page_size: must be greater than zero.",
                    "error_category": "invalid_request",
                },
                separators=(",", ":"),
            )
        if page_size > max_page_size:
            page_size = max_page_size

    if provider == "redshift":
        from dal.redshift import validate_redshift_query

        errors = validate_redshift_query(sql_query)
        if errors:
            return json.dumps(
                {"error": "Redshift query validation failed.", "details": errors},
                separators=(",", ":"),
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
                return json.dumps(
                    {
                        "error": "Execution timed out.",
                        "error_category": "timeout",
                    },
                    separators=(",", ":"),
                )

            raw_last_truncated = getattr(conn, "last_truncated", False)
            last_truncated = raw_last_truncated if isinstance(raw_last_truncated, bool) else False
            raw_last_truncated_reason = getattr(conn, "last_truncated_reason", None)
            last_truncated_reason = (
                raw_last_truncated_reason if isinstance(raw_last_truncated_reason, str) else None
            )

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

        if include_columns and not columns:
            columns = _build_columns_from_rows(result_rows)

        is_truncated = bool(last_truncated or safety_truncated or forced_limited)
        partial_reason = last_truncated_reason
        if partial_reason is None and forced_limited:
            partial_reason = "LIMITED"
        if partial_reason is None and safety_truncated:
            partial_reason = "TRUNCATED"
        if partial_reason is None and is_truncated:
            partial_reason = "TRUNCATED"
        payload = {
            "rows": result_rows,
            "metadata": {
                "is_truncated": is_truncated,
                "row_limit": int(row_limit or 0),
                "rows_returned": len(result_rows),
                "next_page_token": next_token,
                "page_size": effective_page_size,
                "partial_reason": partial_reason,
                **capability_metadata,
            },
        }
        if include_columns:
            payload["columns"] = columns

        return json.dumps(payload, default=str, separators=(",", ":"))

    except asyncpg.PostgresError as e:
        error_message = f"Database Error: {str(e)}"
        payload = {"error": error_message}
        provider = Database.get_query_target_provider()
        category = maybe_classify_error(provider, e)
        if category:
            payload["error_category"] = category
            emit_classified_error(provider, "execute_sql_query", category, e)
        return json.dumps(payload)
    except Exception as e:
        error_message = f"Execution Error: {str(e)}"
        payload = {
            "error": error_message,
            "error_type": type(e).__name__,
        }
        provider = Database.get_query_target_provider()
        category = maybe_classify_error(provider, e)
        if category:
            payload["error_category"] = category
            emit_classified_error(provider, "execute_sql_query", category, e)
        return json.dumps(payload)
