"""MCP tool: execute_sql_query - Execute read-only SQL queries."""

import asyncio
import json
import re
from typing import Optional

import asyncpg

from dal.database import Database
from dal.error_classification import emit_classified_error, maybe_classify_error
from dal.util.column_metadata import build_column_meta
from dal.util.row_limits import get_sync_max_rows

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
            return
    executor = getattr(conn, "executor", None)
    if executor is None:
        return
    cancel_executor = getattr(executor, "cancel", None)
    if callable(cancel_executor) and job_id:
        try:
            await cancel_executor(job_id)
        except Exception:
            return


async def handler(
    sql_query: str,
    tenant_id: Optional[int] = None,
    params: Optional[list] = None,
    include_columns: bool = False,
    timeout_seconds: Optional[float] = None,
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

    def _unsupported_capability_response(required_capability: str) -> str:
        return json.dumps(
            {
                "error": f"Requested capability is not supported: {required_capability}.",
                "error_category": "unsupported_capability",
                "required_capability": required_capability,
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

    caps = Database.get_query_target_capabilities()
    if include_columns and not caps.supports_column_metadata:
        return _unsupported_capability_response("column_metadata")
    if timeout_seconds and timeout_seconds > 0 and caps.execution_model == "async":
        if not caps.supports_cancel:
            return _unsupported_capability_response("async_cancel")

    if Database.get_query_target_provider() == "redshift":
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
        async with Database.get_connection(tenant_id, read_only=True) as conn:
            row_limit = _resolve_row_limit(conn)

            async def _fetch_rows():
                nonlocal columns
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
                if timeout_seconds and timeout_seconds > 0:
                    result = await asyncio.wait_for(_fetch_rows(), timeout=timeout_seconds)
                else:
                    result = await _fetch_rows()
            except asyncio.TimeoutError:
                await _cancel_best_effort(conn)
                return json.dumps(
                    {
                        "error": "Execution timed out.",
                        "error_category": "timeout",
                    },
                    separators=(",", ":"),
                )

            raw_last_truncated = getattr(conn, "last_truncated", False)
            last_truncated = raw_last_truncated if isinstance(raw_last_truncated, bool) else False

        # Size Safety Valve
        safety_limit = 1000
        safety_truncated = False
        if len(result) > safety_limit:
            result = result[:safety_limit]
            safety_truncated = True
            row_limit = safety_limit

        if include_columns and not columns:
            columns = _build_columns_from_rows(result)

        is_truncated = bool(last_truncated or safety_truncated)
        payload = {
            "rows": result,
            "metadata": {
                "is_truncated": is_truncated,
                "row_limit": int(row_limit or 0),
                "rows_returned": len(result),
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
        payload = {"error": error_message}
        provider = Database.get_query_target_provider()
        category = maybe_classify_error(provider, e)
        if category:
            payload["error_category"] = category
            emit_classified_error(provider, "execute_sql_query", category, e)
        return json.dumps(payload)
