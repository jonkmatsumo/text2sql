"""MCP tool: get_table_schema - Retrieve schema for tables."""

import json
from typing import Optional

from dal.database import Database

TOOL_NAME = "get_table_schema"


async def handler(table_names: list[str], tenant_id: Optional[int] = None) -> str:
    """Retrieve the schema (columns, data types, foreign keys) for a list of tables.

    Args:
        table_names: A list of exact table names (e.g. ['film', 'actor']).
        tenant_id: Optional tenant identifier (not required for schema queries).

    Returns:
        JSON array of table schema objects with columns and foreign keys.
    """
    import time

    start_time = time.monotonic()

    store = Database.get_metadata_store()
    schema_list = []

    for table in table_names:
        try:
            definition_json = await store.get_table_definition(table)
            definition = json.loads(definition_json)
            schema_list.append(definition)
        except Exception as e:
            # Capture error for specific table rather than silent skip
            schema_list.append(
                {
                    "table_name": table,
                    "error": str(e),
                    "status": "not_found" if "not found" in str(e).lower() else "error",
                }
            )

    execution_time_ms = (time.monotonic() - start_time) * 1000

    from common.models.tool_envelopes import GenericToolMetadata, ToolResponseEnvelope

    envelope = ToolResponseEnvelope(
        result=schema_list,
        metadata=GenericToolMetadata(
            provider=Database.get_query_target_provider(), execution_time_ms=execution_time_ms
        ),
    )
    return envelope.model_dump_json(exclude_none=True)
