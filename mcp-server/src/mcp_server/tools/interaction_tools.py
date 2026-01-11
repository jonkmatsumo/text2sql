from typing import List, Optional

from mcp_server.config.database import Database
from mcp_server.dal.interaction import InteractionDAL


async def create_interaction_tool(
    conversation_id: Optional[str],
    schema_snapshot_id: str,
    user_nlq_text: str,
    model_version: Optional[str] = None,
    prompt_version: Optional[str] = None,
    trace_id: Optional[str] = None,
) -> str:
    """
    Log the start of a user interaction.

    Returns: interaction_id (UUID string)
    """
    async with Database.get_connection() as conn:
        dal = InteractionDAL(conn)
        return await dal.create_interaction(
            conversation_id,
            schema_snapshot_id,
            user_nlq_text,
            model_version,
            prompt_version,
            trace_id,
        )


async def update_interaction_tool(
    interaction_id: str,
    generated_sql: Optional[str] = None,
    response_payload: Optional[str] = None,  # JSON string
    execution_status: str = "SUCCESS",
    error_type: Optional[str] = None,
    tables_used: Optional[List[str]] = None,
) -> str:
    """
    Log the result of an interaction.

    Returns: "OK"
    """
    async with Database.get_connection() as conn:
        dal = InteractionDAL(conn)
        await dal.update_interaction_result(
            interaction_id,
            generated_sql,
            response_payload,
            execution_status,
            error_type,
            tables_used,
        )
    return "OK"
