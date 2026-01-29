"""MCP tool for schema hydration."""

import json

from mcp_server.services.ops.maintenance import MaintenanceService


async def handler() -> str:
    """Trigger schema hydration from Postgres to Graph store."""
    try:
        results = []
        async for msg in MaintenanceService.hydrate_schema():
            results.append(msg)
        return json.dumps({"success": True, "logs": results})
    except Exception as e:
        return json.dumps({"success": False, "error": str(e)})
