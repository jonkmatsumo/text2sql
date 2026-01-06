"""MCP tool implementations for database operations."""

import json
from typing import Optional

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
