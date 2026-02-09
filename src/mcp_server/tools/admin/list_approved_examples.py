"""MCP tool: list_approved_examples - List all few-shot examples from Registry."""

from typing import Optional

TOOL_NAME = "list_approved_examples"


async def handler(tenant_id: Optional[int] = None, limit: int = 50) -> str:
    """List all few-shot examples in the Registry.

    Args:
        tenant_id: Optional tenant identifier to filter by.
        limit: Maximum number of examples to return (default: 50).

    Returns:
        List of example dictionaries with signature_key, question, sql_query, status, created_at.
    """
    import time

    from common.models.tool_envelopes import GenericToolMetadata, ToolResponseEnvelope
    from mcp_server.services.registry.service import RegistryService
    from mcp_server.utils.validation import validate_limit

    if err := validate_limit(limit, TOOL_NAME):
        return err

    start_time = time.monotonic()

    from mcp_server.utils.auth import validate_role

    if err := validate_role("ADMIN_ROLE", TOOL_NAME):
        return err

    t_id = int(tenant_id) if tenant_id is not None else None
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
