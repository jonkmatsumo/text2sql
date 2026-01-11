"""MCP tool: get_table_schema - Retrieve schema for tables."""

import json
from typing import Optional

from mcp_server.config.database import Database

TOOL_NAME = "get_table_schema"


async def handler(table_names: list[str], tenant_id: Optional[int] = None) -> str:
    """Retrieve the schema (columns, data types, foreign keys) for a list of tables.

    Args:
        table_names: A list of exact table names (e.g. ['film', 'actor']).
        tenant_id: Optional tenant identifier (not required for schema queries).

    Returns:
        JSON array of table schema objects with columns and foreign keys.
    """
    store = Database.get_metadata_store()
    schema_list = []

    for table in table_names:
        try:
            definition_json = await store.get_table_definition(table)
            definition = json.loads(definition_json)
            schema_list.append(definition)
        except Exception:
            # Silently skip tables that error (e.g. don't exist)
            continue

    return json.dumps(schema_list, separators=(",", ":"))
