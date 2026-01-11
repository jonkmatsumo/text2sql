"""MCP tool: list_approved_examples - List all few-shot examples from Registry."""

from typing import List, Optional

TOOL_NAME = "list_approved_examples"


async def handler(tenant_id: Optional[int] = None, limit: int = 50) -> List[dict]:
    """List all few-shot examples in the Registry.

    Args:
        tenant_id: Optional tenant identifier to filter by.
        limit: Maximum number of examples to return (default: 50).

    Returns:
        List of example dictionaries with signature_key, question, sql_query, status, created_at.
    """
    from mcp_server.services.registry.service import RegistryService

    t_id = int(tenant_id) if tenant_id is not None else None
    pairs = await RegistryService.list_examples(tenant_id=t_id, limit=limit)

    return [
        {
            "signature_key": p.signature_key,
            "question": p.question,
            "sql_query": p.sql_query,
            "status": p.status,
            "created_at": p.created_at.isoformat() if p.created_at else None,
        }
        for p in pairs
    ]
