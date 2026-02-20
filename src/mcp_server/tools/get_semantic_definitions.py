"""MCP tool: get_semantic_definitions - Retrieve business metric definitions."""

from dal.database import Database

TOOL_NAME = "get_semantic_definitions"
TOOL_DESCRIPTION = "Retrieve business metric definitions from the semantic layer."


async def handler(terms: list[str], tenant_id: int) -> str:
    """Retrieve business metric definitions from the semantic layer.

    Authorization:
        Requires 'SQL_USER_ROLE' (or higher).

    Data Access:
        Read-only access to the public semantic_definitions table.

    Failure Modes:
        - Unauthorized: If the required role is missing.
        - Database Error: If the semantic layer table is unavailable.

    Args:
        terms: List of term names to look up (e.g. ['High Value Customer', 'Churned']).
        tenant_id: Tenant identifier.

    Returns:
        JSON string mapping term names to their definitions and SQL logic.
    """
    import time

    from common.models.tool_envelopes import GenericToolMetadata, ToolResponseEnvelope
    from mcp_server.utils.auth import validate_role
    from mcp_server.utils.validation import require_tenant_id

    if err := validate_role("SQL_USER_ROLE", TOOL_NAME, tenant_id=tenant_id):
        return err
    start_time = time.monotonic()

    if err := require_tenant_id(tenant_id, TOOL_NAME):
        return err

    if not terms:
        return ToolResponseEnvelope(
            result={},
            metadata=GenericToolMetadata(provider="semantic_layer", execution_time_ms=0),
        ).model_dump_json(exclude_none=True)

    # Build parameterized query for multiple terms
    placeholders = ",".join([f"${i+1}" for i in range(len(terms))])
    query = f"""
        SELECT term_name, definition, sql_logic
        FROM public.semantic_definitions
        WHERE term_name = ANY(ARRAY[{placeholders}])
    """

    async with Database.get_connection(tenant_id) as conn:
        rows = await conn.fetch(query, *terms)

        result = {
            row["term_name"]: {
                "definition": row["definition"],
                "sql_logic": row["sql_logic"],
            }
            for row in rows
        }

        execution_time_ms = (time.monotonic() - start_time) * 1000

        return ToolResponseEnvelope(
            result=result,
            metadata=GenericToolMetadata(
                provider="semantic_layer", execution_time_ms=execution_time_ms
            ),
        ).model_dump_json(exclude_none=True)
