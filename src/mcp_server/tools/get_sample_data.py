"""MCP tool: get_sample_data - Get sample rows from a table."""

from typing import Optional

TOOL_NAME = "get_sample_data"


async def handler(table_name: str, limit: int = 3, tenant_id: Optional[int] = None) -> str:
    """Get sample data rows from a table.

    Args:
        table_name: The name of the table.
        limit: Number of rows to return (default: 3).
        tenant_id: Tenant identifier (REQUIRED).

    Returns:
        JSON string of sample data.
    """
    import json
    import time

    from common.models.tool_envelopes import GenericToolMetadata, ToolResponseEnvelope
    from dal.database import Database

    # 1. Enforce Tenant ID
    if tenant_id is None:
        return json.dumps(
            {
                "error": "Tenant ID is required for get_sample_data.",
                "error_category": "invalid_request",
            }
        )

    start_time = time.monotonic()

    # 2. Execute with Tenant Scope
    safe_table = table_name.replace('"', '""')
    query = f'SELECT * FROM "{safe_table}" LIMIT $1'

    async with Database.get_connection(tenant_id=tenant_id) as conn:
        rows = await conn.fetch(query, limit)
        data = [dict(row) for row in rows]

    execution_time_ms = (time.monotonic() - start_time) * 1000

    envelope = ToolResponseEnvelope(
        result=data,
        metadata=GenericToolMetadata(
            provider=Database.get_query_target_provider(),
            execution_time_ms=execution_time_ms,
        ),
    )
    return envelope.model_dump_json(exclude_none=True)
