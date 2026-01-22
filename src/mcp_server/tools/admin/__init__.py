"""Admin tools package for MCP server.

This package contains tools for administrative operations like
reviewing interactions, approving/rejecting queries, and exporting
to the few-shot registry.
"""

from mcp_server.tools.admin.approve_interaction import handler as approve_interaction_handler
from mcp_server.tools.admin.export_approved_to_fewshot import (
    handler as export_approved_to_fewshot_handler,
)
from mcp_server.tools.admin.get_interaction_details import (
    handler as get_interaction_details_handler,
)
from mcp_server.tools.admin.list_approved_examples import handler as list_approved_examples_handler
from mcp_server.tools.admin.list_interactions import handler as list_interactions_handler
from mcp_server.tools.admin.reject_interaction import handler as reject_interaction_handler
from mcp_server.tools.admin.reload_patterns import handler as reload_patterns_handler

__all__ = [
    "list_interactions_handler",
    "get_interaction_details_handler",
    "approve_interaction_handler",
    "reject_interaction_handler",
    "export_approved_to_fewshot_handler",
    "list_approved_examples_handler",
    "reload_patterns_handler",
]
