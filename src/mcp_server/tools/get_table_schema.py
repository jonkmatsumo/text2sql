"""MCP tool: get_table_schema - Retrieve schema for tables."""

import json
from typing import Optional

from dal.database import Database

TOOL_NAME = "get_table_schema"
TOOL_DESCRIPTION = "Retrieve the schema (columns, data types, foreign keys) for a list of tables."


async def handler(
    table_names: list[str], tenant_id: Optional[int] = None, snapshot_id: Optional[str] = None
) -> str:
    """Retrieve the schema (columns, data types, foreign keys) for a list of tables.

    Authorization:
        Requires 'TABLE_ADMIN_ROLE' for execution.

    Data Access:
        Read-only access to the metadata store.

    Failure Modes:
        - Unauthorized: If the required role is missing.
        - Table Not Found: If one or more requested tables do not exist.
        - Table Inaccessible: If the user lacks permissions for a specific table.

    Args:
        table_names: A list of exact table names (e.g. ['film', 'actor']).
        tenant_id: Optional tenant identifier.
        snapshot_id: Optional schema snapshot identifier to verify consistency.

    Returns:
        JSON array of table schema objects with columns and foreign keys.
    """
    import time

    start_time = time.monotonic()

    from mcp_server.utils.auth import validate_role

    if err := validate_role("TABLE_ADMIN_ROLE", TOOL_NAME):
        return err

    store = Database.get_metadata_store()
    schema_list = []

    for table in table_names:
        try:
            definition_json = await store.get_table_definition(table)
            definition = json.loads(definition_json)
            schema_list.append(definition)
        except Exception as e:
            # Differentiate missing vs inaccessible
            error_msg = str(e).lower()
            status = "error"
            if "not found" in error_msg or "does not exist" in error_msg:
                status = "TABLE_NOT_FOUND"
            elif "permission" in error_msg or "access denied" in error_msg:
                status = "TABLE_INACCESSIBLE"

            schema_list.append(
                {
                    "table_name": table,
                    "error": str(e),
                    "status": status,
                }
            )

    execution_time_ms = (time.monotonic() - start_time) * 1000

    from common.models.tool_envelopes import GenericToolMetadata, ToolResponseEnvelope

    envelope = ToolResponseEnvelope(
        result=schema_list,
        metadata=GenericToolMetadata(
            provider=Database.get_query_target_provider(),
            execution_time_ms=execution_time_ms,
            snapshot_id=snapshot_id,
        ),
    )
    return envelope.model_dump_json(exclude_none=True)
