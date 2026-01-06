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
