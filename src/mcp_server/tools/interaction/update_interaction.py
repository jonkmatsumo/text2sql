"""MCP tool: update_interaction - Log the result of an interaction."""

from typing import List, Optional

from dal.factory import get_interaction_store

TOOL_NAME = "update_interaction"
TOOL_DESCRIPTION = "Log the result of an interaction."


async def handler(
    interaction_id: str,
    generated_sql: Optional[str] = None,
    response_payload: Optional[str] = None,
    execution_status: str = "SUCCESS",
    error_type: Optional[str] = None,
    tables_used: Optional[List[str]] = None,
) -> str:
    """Log the result of an interaction.

    Authorization:
        Requires 'SQL_USER_ROLE' (or higher).

    Data Access:
        Write access to the interaction store to update existing records.

    Failure Modes:
        - Unauthorized: If the required role is missing.
        - Not Found: If the interaction_id does not exist.
        - Database Error: If the interaction store is unavailable.

    Args:
        interaction_id: The unique identifier of the interaction.
        generated_sql: The SQL query that was generated.
        response_payload: The final response payload (JSON string or content).
        execution_status: Status of the execution (e.g., SUCCESS, ERROR).
        error_type: Optional error category if execution failed.
        tables_used: List of tables actually used in the final query.

    Returns:
        JSON-encoded ToolResponseEnvelope with "OK" status.
    """
    from mcp_server.utils.envelopes import tool_success_response

    store = get_interaction_store()
    await store.update_interaction_result(
        interaction_id,
        generated_sql,
        response_payload,
        execution_status,
        error_type,
        tables_used,
    )
    return tool_success_response("OK")
