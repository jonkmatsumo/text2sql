"""MCP tool: list_tables - List available tables in the database."""

from typing import Optional

from dal.database import Database

TOOL_NAME = "list_tables"
TOOL_DESCRIPTION = "List available tables in the database."


async def handler(search_term: Optional[str] = None, tenant_id: Optional[int] = None) -> str:
    """List available tables in the database.

    Authorization:
        Requires 'TABLE_ADMIN_ROLE' for execution.

    Data Access:
        Read-only access to the metadata store to retrieve table names.

    Failure Modes:
        - Unauthorized: If the required role is missing.
        - Database Error: If the metadata store is unavailable.

    Args:
        search_term: Optional fuzzy search string to filter table names.
        tenant_id: Optional tenant identifier.

    Returns:
        JSON array of table names as strings.
    """
    import time

    start_time = time.monotonic()

    from mcp_server.utils.auth import validate_role

    if err := validate_role("TABLE_ADMIN_ROLE", TOOL_NAME):
        return err

    store = Database.get_metadata_store()
    tables = await store.list_tables()

    if search_term:
        search_term = search_term.lower()
        tables = [t for t in tables if search_term in t.lower()]

    execution_time_ms = (time.monotonic() - start_time) * 1000

    from common.models.tool_envelopes import GenericToolMetadata, ToolResponseEnvelope

    envelope = ToolResponseEnvelope(
        result=tables,
        metadata=GenericToolMetadata(
            provider=Database.get_query_target_provider(), execution_time_ms=execution_time_ms
        ),
    )
    return envelope.model_dump_json(exclude_none=True)
