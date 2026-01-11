"""MCP tool: get_sample_data - Get sample rows from a table."""

import json
from typing import Optional

from mcp_server.dal.factory import get_schema_introspector

TOOL_NAME = "get_sample_data"


async def handler(table_name: str, limit: int = 3, tenant_id: Optional[int] = None) -> str:
    """Get sample data rows from a table.

    Args:
        table_name: The name of the table.
        limit: Number of rows to return (default: 3).
        tenant_id: Optional tenant identifier (unused).

    Returns:
        JSON string of sample data.
    """
    introspector = get_schema_introspector()
    data = await introspector.get_sample_rows(table_name, limit)
    return json.dumps(data, separators=(",", ":"), default=str)
