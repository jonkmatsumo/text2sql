"""MCP tool: update_interaction - Log the result of an interaction."""

from typing import List, Optional

from dal.factory import get_interaction_store

TOOL_NAME = "update_interaction"


async def handler(
    interaction_id: str,
    generated_sql: Optional[str] = None,
    response_payload: Optional[str] = None,
    execution_status: str = "SUCCESS",
    error_type: Optional[str] = None,
    tables_used: Optional[List[str]] = None,
) -> str:
    """Log the result of an interaction."""
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
