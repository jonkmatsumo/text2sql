"""MCP tool: get_sample_data - Get sample rows from a table."""

from typing import Optional

from dal.factory import get_schema_introspector

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
    import time

    start_time = time.monotonic()

    introspector = get_schema_introspector()
    data = await introspector.get_sample_rows(table_name, limit)

    execution_time_ms = (time.monotonic() - start_time) * 1000

    from common.models.tool_envelopes import GenericToolMetadata, ToolResponseEnvelope
    from dal.database import Database

    envelope = ToolResponseEnvelope(
        result=data,
        metadata=GenericToolMetadata(
            provider=Database.get_query_target_provider(), execution_time_ms=execution_time_ms
        ),
    )
    return envelope.model_dump_json(exclude_none=True)
