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
    "hydrate_schema",
    "reindex_semantic_cache",
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
    from mcp_server.tools.admin.hydrate_schema import handler as hydrate_schema
    from mcp_server.tools.admin.list_approved_examples import handler as list_approved_examples
    from mcp_server.tools.admin.list_interactions import handler as list_interactions
    from mcp_server.tools.admin.reindex_cache import handler as reindex_cache
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

    # Helper for traced registration
    from mcp_server.utils.contract_enforcement import enforce_tool_response_contract
    from mcp_server.utils.tracing import trace_tool

    def register(name, func):
        traced = trace_tool(name)(func)
        wrapped = enforce_tool_response_contract(name)(traced)
        mcp.tool(name=name)(wrapped)

    # Register core retrieval tools
    register("list_tables", list_tables)
    register("get_table_schema", get_table_schema)
    register("get_sample_data", get_sample_data)
    register("search_relevant_tables", search_relevant_tables)
    register("get_semantic_subgraph", get_semantic_subgraph)
    register("get_semantic_definitions", get_semantic_definitions)

    # Register execution tools
    register("execute_sql_query", execute_sql_query)

    # Register validation tools
    register("resolve_ambiguity", resolve_ambiguity)

    # Register cache tools
    register("lookup_cache", lookup_cache)
    register("update_cache", update_cache)
    register("get_few_shot_examples", get_few_shot_examples)
    register("recommend_examples", recommend_examples)

    # Register conversation tools
    register("save_conversation_state", save_conversation_state)
    register("load_conversation_state", load_conversation_state)

    # Register interaction tools
    register("create_interaction", create_interaction)
    register("update_interaction", update_interaction)

    # Register feedback tools
    register("submit_feedback", submit_feedback)

    # Register admin tools (conditional)
    from common.config.env import get_env_bool

    if get_env_bool("MCP_ENABLE_ADMIN_TOOLS", False):
        # Register admin tools
        register("list_interactions", list_interactions)
        register("get_interaction_details", get_interaction_details)
        register("approve_interaction", approve_interaction)
        register("reject_interaction", reject_interaction)
        register("export_approved_to_fewshot", export_approved_to_fewshot)
        register("list_approved_examples", list_approved_examples)
        register("reload_patterns", reload_patterns)
        register("manage_pin_rules", manage_pin_rules)
        register("generate_patterns", generate_patterns)
        register("hydrate_schema", hydrate_schema)
        register("reindex_semantic_cache", reindex_cache)
        logger.info("Admin tools registered")
    else:
        logger.info("Admin tools disabled (MCP_ENABLE_ADMIN_TOOLS=False)")

    logger.info(f"Registered {len(CANONICAL_TOOLS)} tools with MCP server")
