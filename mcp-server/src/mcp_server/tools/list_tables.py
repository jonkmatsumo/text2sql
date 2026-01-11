"""MCP tool: list_tables - List available tables in the database."""

import json
from typing import Optional

from mcp_server.config.database import Database

TOOL_NAME = "list_tables"


async def handler(search_term: Optional[str] = None, tenant_id: Optional[int] = None) -> str:
    """List available tables in the database.

    Use this to discover table names.

    Args:
        search_term: Optional fuzzy search string to filter table names (e.g. 'pay' -> 'payment').
        tenant_id: Optional tenant identifier (not required for schema queries).

    Returns:
        JSON array of table names as strings.
    """
    store = Database.get_metadata_store()
    tables = await store.list_tables()

    if search_term:
        search_term = search_term.lower()
        tables = [t for t in tables if search_term in t.lower()]

    return json.dumps(tables, separators=(",", ":"))
