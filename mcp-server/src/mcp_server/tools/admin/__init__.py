"""Admin tools package for MCP server.

This package contains tools for administrative operations like
reviewing interactions, approving/rejecting queries, and exporting
to the few-shot registry.
"""

from mcp_server.tools.admin.approve_interaction import handler as approve_interaction
from mcp_server.tools.admin.export_approved_to_fewshot import handler as export_approved_to_fewshot
from mcp_server.tools.admin.get_interaction_details import handler as get_interaction_details
from mcp_server.tools.admin.list_approved_examples import handler as list_approved_examples
from mcp_server.tools.admin.list_interactions import handler as list_interactions
from mcp_server.tools.admin.reject_interaction import handler as reject_interaction

__all__ = [
    "list_interactions",
    "get_interaction_details",
    "approve_interaction",
    "reject_interaction",
    "export_approved_to_fewshot",
    "list_approved_examples",
]
