"""MCP tool implementations for database operations."""

import json
import re
from typing import Optional

import asyncpg
from src.db import Database


async def list_tables(search_term: Optional[str] = None) -> str:
    """
    List available tables in the database. Use this to discover table names.

    Args:
        search_term: Optional fuzzy search string to filter table names (e.g. 'pay' -> 'payment').

    Returns:
        JSON array of table names as strings.
    """
    conn = await Database.get_connection()
    try:
        query = """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
        """
        args = []

        if search_term:
            query += " AND table_name ILIKE $1"
            args.append(f"%{search_term}%")

        rows = await conn.fetch(query, *args)
        tables = [row["table_name"] for row in rows]
        return json.dumps(tables, indent=2)
    finally:
        await Database.release_connection(conn)


async def get_table_schema(table_names: list[str]) -> str:
    """
    Retrieve the schema (columns, data types, foreign keys) for a list of tables.

    Args:
        table_names: A list of exact table names (e.g. ['film', 'actor']).

    Returns:
        Markdown-formatted schema documentation.
    """
    conn = await Database.get_connection()
    schema_output = ""

    try:
        for table in table_names:
            # Get Columns
            col_query = """
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns
                WHERE table_name = $1 AND table_schema = 'public'
                ORDER BY ordinal_position
            """
            cols = await conn.fetch(col_query, table)

            if not cols:
                schema_output += f"### Table: {table} (Not Found)\n\n"
                continue

            schema_output += f"### Table: `{table}`\n\n"
            schema_output += "| Column | Type | Nullable |\n|---|---|---|\n"
            for col in cols:
                schema_output += (
                    f"| `{col['column_name']}` | {col['data_type']} | {col['is_nullable']} |\n"
                )

            # Get Foreign Keys
            fk_query = """
                SELECT
                    kcu.column_name,
                    ccu.table_name AS foreign_table_name,
                    ccu.column_name AS foreign_column_name
                FROM information_schema.table_constraints AS tc
                JOIN information_schema.key_column_usage AS kcu
                    ON tc.constraint_name = kcu.constraint_name
                JOIN information_schema.constraint_column_usage AS ccu
                    ON ccu.constraint_name = tc.constraint_name
                WHERE tc.constraint_type = 'FOREIGN KEY'
                    AND tc.table_name = $1
                    AND tc.table_schema = 'public'
            """
            fks = await conn.fetch(fk_query, table)

            if fks:
                schema_output += "\n**Foreign Keys:**\n"
                for fk in fks:
                    fk_col = fk["column_name"]
                    fk_table = fk["foreign_table_name"]
                    fk_ref_col = fk["foreign_column_name"]
                    schema_output += f"- `{fk_col}` → `{fk_table}.{fk_ref_col}`\n"

            schema_output += "\n"

        return schema_output
    finally:
        await Database.release_connection(conn)


async def execute_sql_query(sql_query: str) -> str:
    """
    Execute a valid SQL SELECT statement and return the result as JSON.

    Strictly read-only. Returns error messages as strings for self-correction.

    Args:
        sql_query: A SQL SELECT query string.

    Returns:
        JSON array of result rows, or error message as string.
    """
    # 1. Application-Level Security Check (Pre-flight)
    # Reject mutative keywords to prevent injection attacks or accidental deletion
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

    conn = await Database.get_connection()
    try:
        # 2. Execution
        rows = await conn.fetch(sql_query)

        # 3. Serialization
        # Convert Record objects to dicts, handling non-serializable types
        result = [dict(row) for row in rows]

        # 4. Size Safety Valve
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

        return json.dumps(result, default=str, indent=2)  # default=str handles Date/Decimal types

    except asyncpg.PostgresError as e:
        # Crucial: Return the DB error as a string so the LLM can read it and fix the query
        return f"Database Error: {str(e)}"
    except Exception as e:
        return f"Execution Error: {str(e)}"
    finally:
        await Database.release_connection(conn)
