"""MCP tool: list_approved_examples - List all few-shot examples from Registry."""

TOOL_NAME = "list_approved_examples"
TOOL_DESCRIPTION = "List all few-shot examples in the Registry."


async def handler(tenant_id: int, limit: int = 50) -> str:
    """List all few-shot examples in the Registry.

    Authorization:
        Requires 'ADMIN_ROLE' for execution.

    Data Access:
        Read-only access to the few-shot Registry store. Results can be filtered by tenant_id.

    Failure Modes:
        - Unauthorized: If the required role is missing.
        - Validation Error: If limit is out of bounds.
        - Database Error: If the registry store is unavailable.

    Args:
        tenant_id: Tenant identifier to filter by.
        limit: Maximum number of examples to return (default: 50).

    Returns:
        JSON string containing a list of few-shot example dictionaries.
    """
    import time

    from common.models.tool_envelopes import GenericToolMetadata, ToolResponseEnvelope
    from mcp_server.services.registry.service import RegistryService
    from mcp_server.utils.validation import require_tenant_id, validate_limit

    if err := require_tenant_id(tenant_id, TOOL_NAME):
        return err

    if err := validate_limit(limit, TOOL_NAME):
        return err

    start_time = time.monotonic()

    from mcp_server.utils.auth import require_admin

    if err := require_admin(TOOL_NAME):
        return err

    t_id = int(tenant_id)
    pairs = await RegistryService.list_examples(tenant_id=t_id, limit=limit)

    result_list = [
        {
            "signature_key": p.signature_key,
            "question": p.question,
            "sql_query": p.sql_query,
            "status": p.status,
            "created_at": p.created_at.isoformat() if p.created_at else None,
        }
        for p in pairs
    ]

    execution_time_ms = (time.monotonic() - start_time) * 1000

    return ToolResponseEnvelope(
        result=result_list,
        metadata=GenericToolMetadata(
            provider="registry_service", execution_time_ms=execution_time_ms
        ),
    ).model_dump_json(exclude_none=True)
