"""MCP Server entrypoint for Text 2 SQL Agent.

This module initializes the FastMCP server and registers all database tools.
"""

from contextlib import asynccontextmanager

from dotenv import load_dotenv
from fastmcp import FastMCP
from mcp_server.config.database import Database
from mcp_server.services.cache_service import lookup_cache, update_cache
from mcp_server.services.retrieval_service import get_relevant_examples
from mcp_server.tools import (
    execute_sql_query,
    get_sample_data,
    get_semantic_definitions,
    get_semantic_subgraph,
    get_table_schema,
    list_tables,
    resolve_ambiguity,
    search_relevant_tables,
)
from mcp_server.tools.conversation_tools import load_conversation_state, save_conversation_state

# Load environment variables
load_dotenv()


@asynccontextmanager
async def lifespan(app):
    """Lifespan context manager for database connection pool.

    This ensures the database pool is created in the same event loop
    as the server, avoiding "Event loop is closed" errors.
    """
    # Startup: Initialize database connection pool
    await Database.init()

    # Maintenance: Prune legacy cache entries
    try:
        from mcp_server.services.cache_service import prune_legacy_entries

        count = await prune_legacy_entries()
        if count > 0:
            print(f"🧹 Pruned {count} legacy cache entries on startup")
    except Exception as e:
        print(f"Warning: Cache pruning failed: {e}")

    # Check if schema_embeddings table is empty and try to index
    # This is optional - server should still work without it
    try:
        from mcp_server.services.indexer_service import index_all_tables

        async with Database.get_connection() as conn:
            count = await conn.fetchval("SELECT COUNT(*) FROM public.schema_embeddings")
            if count == 0:
                print("Schema embeddings table is empty. Starting indexing...")
                await index_all_tables()
    except Exception as e:
        print(f"Warning: Index check or indexing failed: {e}")

    yield

    # Shutdown: Close database connection pool
    await Database.close()


# Initialize FastMCP Server with dependencies
mcp = FastMCP("text2sql-agent", lifespan=lifespan)

# --- Register Tools ---

# 1. Retrieval Tools
mcp.tool()(list_tables)
mcp.tool()(get_table_schema)
mcp.tool()(search_relevant_tables)
mcp.tool()(get_semantic_subgraph)
mcp.tool()(get_semantic_definitions)

# 2. Execution Tools
mcp.tool()(execute_sql_query)
mcp.tool()(get_sample_data)

# 3. Validation Tools
mcp.tool()(resolve_ambiguity)

# 4. Conversation Tools (New)
mcp.tool()(save_conversation_state)
mcp.tool()(load_conversation_state)


@mcp.tool()
async def get_few_shot_examples_tool(query: str, limit: int = 3) -> str:
    """Retrieve similar past queries and their corresponding SQL.

    Use this tool to find examples of how to write SQL for similar questions.
    """
    return await get_relevant_examples(query, limit)


@mcp.tool()
async def lookup_cache_tool(query: str, user_id: str = "default_user") -> str:
    """Look up a query in the semantic cache.

    Returns the cached SQL if a semantic match is found, or "MISSING" if not found.
    """
    return await lookup_cache(query, user_id)


@mcp.tool()
async def update_cache_tool(
    query: str, sql: str, thought_process: str, user_id: str = "default_user"
) -> str:
    """Update the semantic cache with a new query-SQL pair.

    Returns "OK" on success.
    """
    return await update_cache(query, sql, thought_process, user_id)


if __name__ == "__main__":
    mcp.run()
