"""MCP server tool integration for LangGraph."""

import os

from dotenv import load_dotenv
from langchain_mcp_adapters.client import MultiServerMCPClient

load_dotenv()

# Default URL for streamable-http transport (endpoint is /mcp)
DEFAULT_MCP_URL = "http://localhost:8000/mcp"


async def get_mcp_tools():
    """
    Connect to the local MCP server via streamable-http.

    The MCP server provides secure, read-only database access through:
    - list_tables: Discover available tables
    - get_table_schema: Retrieve detailed schema metadata
    - execute_sql_query_tool: Execute read-only SQL queries
    - get_semantic_definitions: Retrieve business metric definitions
    - search_relevant_tables: Semantic search for relevant tables

    Returns:
        list: List of LangChain tool wrappers for MCP tools
    """
    mcp_url = os.getenv("MCP_SERVER_URL", DEFAULT_MCP_URL)

    # Use sse transport for streamable-http server (requires text/event-stream)
    client = MultiServerMCPClient(
        {
            "data-layer": {
                "url": mcp_url,
                "transport": "http",
            }
        }
    )

    # Returns tools: list_tables, execute_sql_query_tool, get_semantic_definitions,
    # get_table_schema, search_relevant_tables
    return await client.get_tools()
