"""Central tool response contract version registry."""

from __future__ import annotations

DEFAULT_TOOL_VERSION = "v1"

# Keep this list aligned with mcp_server.tools.registry.CANONICAL_TOOLS.
TOOL_VERSION_REGISTRY: dict[str, str] = {
    "approve_interaction": DEFAULT_TOOL_VERSION,
    "create_interaction": DEFAULT_TOOL_VERSION,
    "execute_sql_query": DEFAULT_TOOL_VERSION,
    "export_approved_to_fewshot": DEFAULT_TOOL_VERSION,
    "generate_patterns": DEFAULT_TOOL_VERSION,
    "get_few_shot_examples": DEFAULT_TOOL_VERSION,
    "get_interaction_details": DEFAULT_TOOL_VERSION,
    "get_sample_data": DEFAULT_TOOL_VERSION,
    "get_semantic_definitions": DEFAULT_TOOL_VERSION,
    "get_semantic_subgraph": DEFAULT_TOOL_VERSION,
    "get_table_schema": DEFAULT_TOOL_VERSION,
    "hydrate_schema": DEFAULT_TOOL_VERSION,
    "list_approved_examples": DEFAULT_TOOL_VERSION,
    "list_interactions": DEFAULT_TOOL_VERSION,
    "list_tables": DEFAULT_TOOL_VERSION,
    "load_conversation_state": DEFAULT_TOOL_VERSION,
    "lookup_cache": DEFAULT_TOOL_VERSION,
    "manage_pin_rules": DEFAULT_TOOL_VERSION,
    "recommend_examples": DEFAULT_TOOL_VERSION,
    "reindex_semantic_cache": DEFAULT_TOOL_VERSION,
    "reject_interaction": DEFAULT_TOOL_VERSION,
    "reload_patterns": DEFAULT_TOOL_VERSION,
    "resolve_ambiguity": DEFAULT_TOOL_VERSION,
    "save_conversation_state": DEFAULT_TOOL_VERSION,
    "search_relevant_tables": DEFAULT_TOOL_VERSION,
    "submit_feedback": DEFAULT_TOOL_VERSION,
    "update_cache": DEFAULT_TOOL_VERSION,
    "update_interaction": DEFAULT_TOOL_VERSION,
}


def get_tool_version(tool_name: str | None) -> str:
    """Resolve the configured contract version for a tool."""
    if not isinstance(tool_name, str):
        return DEFAULT_TOOL_VERSION
    normalized = tool_name.strip()
    if not normalized:
        return DEFAULT_TOOL_VERSION
    return TOOL_VERSION_REGISTRY.get(normalized, DEFAULT_TOOL_VERSION)
