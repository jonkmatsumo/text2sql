"""MCP tool for semantic cache re-indexing."""

import json

from mcp_server.services.ops.maintenance import MaintenanceService


async def handler() -> str:
    """Trigger re-indexing of the semantic cache."""
    try:
        results = []
        async for msg in MaintenanceService.reindex_cache():
            results.append(msg)
        return json.dumps({"success": True, "logs": results})
    except Exception as e:
        return json.dumps({"success": False, "error": str(e)})
