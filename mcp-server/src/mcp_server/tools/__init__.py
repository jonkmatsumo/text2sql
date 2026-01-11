"""Tools package for MCP server.

This package contains all MCP tool implementations organized by function:
- Core tools: list_tables, get_table_schema, get_sample_data, etc.
- Admin tools: list_interactions, approve_interaction, etc.
- Feedback tools: submit_feedback
- Interaction tools: create_interaction, update_interaction
- Conversation tools: save_conversation_state, load_conversation_state

Each tool is implemented in its own module with:
- TOOL_NAME: The canonical name (must not end with "_tool")
- handler: The function implementation
"""

# Re-export admin tools
from mcp_server.tools.admin import (
    approve_interaction,
    export_approved_to_fewshot,
    get_interaction_details,
    list_approved_examples,
    list_interactions,
    reject_interaction,
)

# Re-export conversation tools
from mcp_server.tools.conversation import load_conversation_state, save_conversation_state

# Re-export for backwards compatibility with existing imports
from mcp_server.tools.execute_sql_query import handler as execute_sql_query

# Re-export feedback tools
from mcp_server.tools.feedback import submit_feedback
from mcp_server.tools.get_few_shot_examples import handler as get_few_shot_examples
from mcp_server.tools.get_sample_data import handler as get_sample_data
from mcp_server.tools.get_semantic_definitions import handler as get_semantic_definitions
from mcp_server.tools.get_semantic_subgraph import handler as get_semantic_subgraph
from mcp_server.tools.get_table_schema import handler as get_table_schema

# Re-export interaction tools
from mcp_server.tools.interaction import create_interaction, update_interaction
from mcp_server.tools.list_tables import handler as list_tables
from mcp_server.tools.lookup_cache import handler as lookup_cache
from mcp_server.tools.registry import CANONICAL_TOOLS, get_all_tool_names, register_all
from mcp_server.tools.resolve_ambiguity import handler as resolve_ambiguity
from mcp_server.tools.search_relevant_tables import handler as search_relevant_tables
from mcp_server.tools.update_cache import handler as update_cache

__all__ = [
    # Registry functions
    "register_all",
    "get_all_tool_names",
    "CANONICAL_TOOLS",
    # Core retrieval tools
    "list_tables",
    "get_table_schema",
    "get_sample_data",
    "search_relevant_tables",
    "get_semantic_subgraph",
    "get_semantic_definitions",
    # Execution tools
    "execute_sql_query",
    # Validation tools
    "resolve_ambiguity",
    # Cache tools
    "lookup_cache",
    "update_cache",
    "get_few_shot_examples",
    # Conversation tools
    "save_conversation_state",
    "load_conversation_state",
    # Interaction tools
    "create_interaction",
    "update_interaction",
    # Feedback tools
    "submit_feedback",
    # Admin tools
    "list_interactions",
    "get_interaction_details",
    "approve_interaction",
    "reject_interaction",
    "export_approved_to_fewshot",
    "list_approved_examples",
]
