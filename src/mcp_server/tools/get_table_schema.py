"""MCP tool: get_table_schema - Retrieve schema for tables."""

import json
from typing import Optional

from dal.database import Database

TOOL_NAME = "get_table_schema"
TOOL_DESCRIPTION = "Retrieve the schema (columns, data types, foreign keys) for a list of tables."


async def handler(table_names: list[str], tenant_id: int, snapshot_id: Optional[str] = None) -> str:
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
        tenant_id: Tenant identifier.
        snapshot_id: Optional schema snapshot identifier to verify consistency.

    Returns:
        JSON array of table schema objects with columns and foreign keys.
    """
    import time

    start_time = time.monotonic()

    from common.models.error_metadata import ErrorCategory
    from mcp_server.utils.auth import validate_role
    from mcp_server.utils.errors import build_error_metadata
    from mcp_server.utils.validation import require_tenant_id

    if err := require_tenant_id(tenant_id, TOOL_NAME):
        return err

    if err := validate_role("TABLE_ADMIN_ROLE", TOOL_NAME):
        return err

    store = Database.get_metadata_store()
    schema_list = []
    provider = Database.get_query_target_provider()

    for table in table_names:
        try:
            definition_json = await store.get_table_definition(table, tenant_id=tenant_id)
            definition = json.loads(definition_json)
            schema_list.append(definition)
        except Exception as e:
            # Differentiate missing vs inaccessible
            error_msg = str(e).lower()
            status = "error"
<<<<<<< HEAD
            category = "internal"
=======
            category = ErrorCategory.DEPENDENCY_FAILURE
>>>>>>> 03e1e11b (feat(obs): add explicit read-only enforcement telemetry across DAL and MCP)
            message = "Failed to retrieve table schema."
            if "not found" in error_msg or "does not exist" in error_msg:
                status = "TABLE_NOT_FOUND"
                category = ErrorCategory.INVALID_REQUEST
                message = "Requested table was not found."
            elif "permission" in error_msg or "access denied" in error_msg:
                status = "TABLE_INACCESSIBLE"
                category = ErrorCategory.AUTH
                message = "Requested table is inaccessible."

            schema_list.append(
                {
                    "table_name": table,
                    "error": build_error_metadata(
                        message=message,
                        category=category,
                        provider=provider,
                        code=status,
                    ).to_dict(),
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
            items_returned=len(schema_list),
        ),
    )
    return envelope.model_dump_json(exclude_none=True)
