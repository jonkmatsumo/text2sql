"""MCP Server entrypoint for Text 2 SQL Agent.

This module initializes the FastMCP server and registers all database tools
via the central registry.
"""

from contextlib import asynccontextmanager

from dotenv import load_dotenv
from fastmcp import FastMCP
from mcp_server.config.database import Database
from mcp_server.tools.registry import register_all

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

# Register all tools via the central registry
register_all(mcp)


if __name__ == "__main__":
    import os

    # Respect transport and host/port from environment for containerized use
    transport = os.getenv("MCP_TRANSPORT", "stdio").lower()
    host = os.getenv("MCP_HOST", "0.0.0.0")
    port = int(os.getenv("MCP_PORT", "8000"))

    if transport in ("sse", "http", "streamable-http"):
        # We standardize on sse transport to be compatible with langchain-mcp-adapters
        # which does not yet support the session requirements of streamable-http.
        print(f"🚀 Starting MCP server in sse mode on {host}:{port}/messages")
        mcp.run(transport="sse", host=host, port=port, path="/messages")
    else:
        mcp.run(transport="stdio")
