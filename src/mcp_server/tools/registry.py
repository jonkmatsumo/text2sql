"""Central registry for MCP tools.

This module provides a single point of registration for all MCP tools.
It collects tool modules and registers them with the FastMCP server.
"""

import logging
from typing import TYPE_CHECKING, List, Set

if TYPE_CHECKING:
    from fastmcp import FastMCP

logger = logging.getLogger(__name__)

# Canonical tool names (without _tool suffix)
# This is the authoritative list of all public tools
CANONICAL_TOOLS: Set[str] = {
    # Core schema/retrieval tools
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
    "recommend_examples",
    "reload_patterns",
    "manage_pin_rules",
    "generate_patterns",
}


def get_all_tool_names() -> List[str]:
    """Return list of all canonical tool names."""
    return sorted(CANONICAL_TOOLS)


def validate_tool_names() -> bool:
    """Validate that no canonical tool names end with '_tool'.

    Returns:
        True if all names are valid, raises ValueError otherwise.
    """
    invalid = [name for name in CANONICAL_TOOLS if name.endswith("_tool")]
    if invalid:
        raise ValueError(f"Tool names must not end with '_tool': {invalid}")
    return True


def register_all(mcp: "FastMCP") -> None:
    """Register all tools with the MCP server.

    Args:
        mcp: FastMCP server instance
    """
    # Validate tool names before registration
    validate_tool_names()

    # Import tool handlers
    # Admin tools
    from mcp_server.tools.admin.approve_interaction import handler as approve_interaction
    from mcp_server.tools.admin.export_approved_to_fewshot import (
        handler as export_approved_to_fewshot,
    )
    from mcp_server.tools.admin.generate_patterns import handler as generate_patterns
    from mcp_server.tools.admin.get_interaction_details import handler as get_interaction_details
    from mcp_server.tools.admin.list_approved_examples import handler as list_approved_examples
    from mcp_server.tools.admin.list_interactions import handler as list_interactions
    from mcp_server.tools.admin.reject_interaction import handler as reject_interaction
    from mcp_server.tools.admin.reload_patterns import handler as reload_patterns

    # Conversation tools
    from mcp_server.tools.conversation.load_conversation_state import (
        handler as load_conversation_state,
    )
    from mcp_server.tools.conversation.save_conversation_state import (
        handler as save_conversation_state,
    )
    from mcp_server.tools.execute_sql_query import handler as execute_sql_query

    # Feedback tools
    from mcp_server.tools.feedback.submit_feedback import handler as submit_feedback
    from mcp_server.tools.get_few_shot_examples import handler as get_few_shot_examples
    from mcp_server.tools.get_sample_data import handler as get_sample_data
    from mcp_server.tools.get_semantic_definitions import handler as get_semantic_definitions
    from mcp_server.tools.get_semantic_subgraph import handler as get_semantic_subgraph
    from mcp_server.tools.get_table_schema import handler as get_table_schema

    # Interaction tools
    from mcp_server.tools.interaction.create_interaction import handler as create_interaction
    from mcp_server.tools.interaction.update_interaction import handler as update_interaction
    from mcp_server.tools.list_tables import handler as list_tables
    from mcp_server.tools.lookup_cache import handler as lookup_cache
    from mcp_server.tools.manage_pin_rules import handler as manage_pin_rules
    from mcp_server.tools.recommend_examples import handler as recommend_examples
    from mcp_server.tools.resolve_ambiguity import handler as resolve_ambiguity
    from mcp_server.tools.search_relevant_tables import handler as search_relevant_tables
    from mcp_server.tools.update_cache import handler as update_cache

    # Register core retrieval tools
    mcp.tool(name="list_tables")(list_tables)
    mcp.tool(name="get_table_schema")(get_table_schema)
    mcp.tool(name="get_sample_data")(get_sample_data)
    mcp.tool(name="search_relevant_tables")(search_relevant_tables)
    mcp.tool(name="get_semantic_subgraph")(get_semantic_subgraph)
    mcp.tool(name="get_semantic_definitions")(get_semantic_definitions)

    # Register execution tools
    mcp.tool(name="execute_sql_query")(execute_sql_query)

    # Register validation tools
    mcp.tool(name="resolve_ambiguity")(resolve_ambiguity)

    # Register cache tools
    mcp.tool(name="lookup_cache")(lookup_cache)
    mcp.tool(name="update_cache")(update_cache)
    mcp.tool(name="get_few_shot_examples")(get_few_shot_examples)
    mcp.tool(name="recommend_examples")(recommend_examples)

    # Register conversation tools
    mcp.tool(name="save_conversation_state")(save_conversation_state)
    mcp.tool(name="load_conversation_state")(load_conversation_state)

    # Register interaction tools
    mcp.tool(name="create_interaction")(create_interaction)
    mcp.tool(name="update_interaction")(update_interaction)

    # Register feedback tools
    mcp.tool(name="submit_feedback")(submit_feedback)

    # Register admin tools
    mcp.tool(name="list_interactions")(list_interactions)
    mcp.tool(name="get_interaction_details")(get_interaction_details)
    mcp.tool(name="approve_interaction")(approve_interaction)
    mcp.tool(name="reject_interaction")(reject_interaction)
    mcp.tool(name="export_approved_to_fewshot")(export_approved_to_fewshot)
    mcp.tool(name="list_approved_examples")(list_approved_examples)
    mcp.tool(name="reload_patterns")(reload_patterns)
    mcp.tool(name="manage_pin_rules")(manage_pin_rules)
    mcp.tool(name="generate_patterns")(generate_patterns)

    logger.info(f"Registered {len(CANONICAL_TOOLS)} tools with MCP server")
