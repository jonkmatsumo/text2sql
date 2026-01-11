"""MCP tool: update_interaction - Log the result of an interaction."""

from typing import List, Optional

from mcp_server.dal.factory import get_interaction_store

TOOL_NAME = "update_interaction"


async def handler(
    interaction_id: str,
    generated_sql: Optional[str] = None,
    response_payload: Optional[str] = None,
    execution_status: str = "SUCCESS",
    error_type: Optional[str] = None,
    tables_used: Optional[List[str]] = None,
) -> str:
    """Log the result of an interaction.

    Args:
        interaction_id: The unique identifier of the interaction.
        generated_sql: The SQL query that was generated.
        response_payload: JSON string of the response payload.
        execution_status: Status of execution (default: "SUCCESS").
        error_type: Optional error type if execution failed.
        tables_used: Optional list of tables used in the query.

    Returns:
        "OK" on success.
    """
    store = get_interaction_store()
    await store.update_interaction_result(
        interaction_id,
        generated_sql,
        response_payload,
        execution_status,
        error_type,
        tables_used,
    )
    return "OK"
