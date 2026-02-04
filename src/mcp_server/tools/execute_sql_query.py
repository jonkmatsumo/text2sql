"""MCP tool: execute_sql_query - Execute read-only SQL queries."""

import json
import re
from typing import Optional

import asyncpg

from dal.database import Database
from dal.error_classification import emit_classified_error, maybe_classify_error

TOOL_NAME = "execute_sql_query"


def _build_columns_from_rows(rows: list[dict]) -> list[dict]:
    if not rows:
        return []
    first_row = rows[0]
    return [{"name": key, "type": "unknown"} for key in first_row.keys()]


async def handler(
    sql_query: str,
    tenant_id: Optional[int] = None,
    params: Optional[list] = None,
    include_columns: bool = False,
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
        if include_columns:
            query_result = await Database.fetch_query(
                sql_query,
                tenant_id=tenant_id,
                params=params,
                include_columns=True,
            )
            result = query_result.rows
            columns = query_result.columns
        else:
            async with Database.get_connection(tenant_id, read_only=True) as conn:
                if params:
                    rows = await conn.fetch(sql_query, *params)
                else:
                    rows = await conn.fetch(sql_query)

                result = [dict(row) for row in rows]

        # Size Safety Valve
        if len(result) > 1000:
            error_msg = (
                f"Result set too large ({len(result)} rows). "
                "Please add a LIMIT clause to your query."
            )
            return json.dumps(
                {
                    "error": error_msg,
                    "truncated_result": result[:1000],
                },
                default=str,
            )

        if include_columns:
            if not columns:
                columns = _build_columns_from_rows(result)
            return json.dumps(
                {"rows": result, "columns": columns},
                default=str,
                separators=(",", ":"),
            )

        return json.dumps(result, default=str, separators=(",", ":"))

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
