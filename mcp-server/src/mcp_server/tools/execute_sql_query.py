"""MCP tool: execute_sql_query - Execute read-only SQL queries."""

import json
import re
from typing import Optional

import asyncpg

from dal.database import Database

TOOL_NAME = "execute_sql_query"


async def handler(
    sql_query: str, tenant_id: Optional[int] = None, params: Optional[list] = None
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
    # Reject mutative keywords to prevent injection attacks
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

    async with Database.get_connection(tenant_id) as conn:
        try:
            # Execution
            if params:
                rows = await conn.fetch(sql_query, *params)
            else:
                rows = await conn.fetch(sql_query)

            # Serialization
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

            return json.dumps(result, default=str, separators=(",", ":"))

        except asyncpg.PostgresError as e:
            return json.dumps({"error": f"Database Error: {str(e)}"})
        except Exception as e:
            return json.dumps({"error": f"Execution Error: {str(e)}"})
