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
from mcp_server.tools.admin import approve_interaction as approve_interaction_handler
from mcp_server.tools.admin import export_approved_to_fewshot as export_approved_to_fewshot_handler
from mcp_server.tools.admin import get_interaction_details as get_interaction_details_handler
from mcp_server.tools.admin import list_approved_examples as list_approved_examples_handler
from mcp_server.tools.admin import list_interactions as list_interactions_handler
from mcp_server.tools.admin import reject_interaction as reject_interaction_handler
from mcp_server.tools.admin.generate_patterns import handler as generate_patterns_handler

# Re-export conversation tools
from mcp_server.tools.conversation import load_conversation_state as load_conversation_state_handler
from mcp_server.tools.conversation import save_conversation_state as save_conversation_state_handler

# Re-export for backwards compatibility with existing imports
from mcp_server.tools.execute_sql_query import handler as execute_sql_query_handler
from mcp_server.tools.feedback import submit_feedback as submit_feedback_handler
from mcp_server.tools.get_few_shot_examples import handler as get_few_shot_examples_handler
from mcp_server.tools.get_sample_data import handler as get_sample_data_handler
from mcp_server.tools.get_semantic_definitions import handler as get_semantic_definitions_handler
from mcp_server.tools.get_semantic_subgraph import handler as get_semantic_subgraph_handler
from mcp_server.tools.get_table_schema import handler as get_table_schema_handler

# Re-export interaction tools
from mcp_server.tools.interaction import create_interaction as create_interaction_handler
from mcp_server.tools.interaction import update_interaction as update_interaction_handler
from mcp_server.tools.list_tables import handler as list_tables_handler
from mcp_server.tools.lookup_cache import handler as lookup_cache_handler
from mcp_server.tools.registry import CANONICAL_TOOLS, get_all_tool_names, register_all
from mcp_server.tools.resolve_ambiguity import handler as resolve_ambiguity_handler
from mcp_server.tools.search_relevant_tables import handler as search_relevant_tables_handler
from mcp_server.tools.update_cache import handler as update_cache_handler

__all__ = [
    # Registry functions
    "register_all",
    "get_all_tool_names",
    "CANONICAL_TOOLS",
    # Core retrieval tools
    "list_tables_handler",
    "get_table_schema_handler",
    "get_sample_data_handler",
    "search_relevant_tables_handler",
    "get_semantic_subgraph_handler",
    "get_semantic_definitions_handler",
    # Execution tools
    "execute_sql_query_handler",
    # Validation tools
    "resolve_ambiguity_handler",
    # Cache tools
    "lookup_cache_handler",
    "update_cache_handler",
    "get_few_shot_examples_handler",
    # Conversation tools
    "save_conversation_state_handler",
    "load_conversation_state_handler",
    # Interaction tools
    "create_interaction_handler",
    "update_interaction_handler",
    # Feedback tools
    "submit_feedback_handler",
    # Admin tools
    "list_interactions_handler",
    "get_interaction_details_handler",
    "approve_interaction_handler",
    "reject_interaction_handler",
    "export_approved_to_fewshot_handler",
    "list_approved_examples_handler",
    "generate_patterns_handler",
]

# Re-exports for backwards compatibility are handled via __all__
