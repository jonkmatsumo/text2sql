"""MCP server tool integration for LangGraph."""

from contextlib import asynccontextmanager
from typing import Any

from dotenv import load_dotenv
from langchain_mcp_adapters.client import MultiServerMCPClient

from common.config.env import get_env_str

load_dotenv()

# Default URL for sse transport (endpoint is /messages)
DEFAULT_MCP_URL = "http://localhost:8000/messages"


async def get_mcp_tools():
    """
    Connect to the local MCP server via streamable-http.

    The MCP server provides secure, read-only database access through:
    - list_tables: Discover available tables
    - get_table_schema: Retrieve detailed schema metadata
    - execute_sql_query: Execute read-only SQL queries
    - get_semantic_definitions: Retrieve business metric definitions
    - search_relevant_tables: Semantic search for relevant tables

    Returns:
        list: List of LangChain tool wrappers for MCP tools
    """
    mcp_url = get_env_str("MCP_SERVER_URL", DEFAULT_MCP_URL)

    # Use sse transport for compatibility
    client = MultiServerMCPClient(
        {
            "data-layer": {
                "url": mcp_url,
                "transport": "sse",
            }
        }
    )

    # Returns tools: list_tables, execute_sql_query, get_semantic_definitions,
    # get_table_schema, search_relevant_tables
    return await client.get_tools()


@asynccontextmanager
async def mcp_tools_context():
    """Context manager for backward compatibility and future stability."""
    # Since MultiServerMCPClient 0.1.0 doesn't support context manager directly,
    # we just yield the tools for now.
    tools = await get_mcp_tools()
    yield tools


def unpack_mcp_result(result: Any) -> Any:
    """Unpack standardized MCP content list/dict into raw value."""
    import json

    # LangChain MCP adapter returns a list of dicts like [{'type': 'text', 'text': '...'}]
    if isinstance(result, list) and result and isinstance(result[0], dict) and "type" in result[0]:
        text_content = ""
        for item in result:
            if item.get("type") == "text":
                text_content += item.get("text", "")

        # Try parsing as JSON if it looks like a JSON object/list
        stripped = text_content.strip()
        if (stripped.startswith("{") and stripped.endswith("}")) or (
            stripped.startswith("[") and stripped.endswith("]")
        ):
            try:
                return json.loads(text_content)
            except json.JSONDecodeError:
                pass
        return text_content

    return result
