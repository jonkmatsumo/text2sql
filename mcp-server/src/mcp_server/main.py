"""MCP Server entrypoint for Text 2 SQL Agent.

This module initializes the FastMCP server and registers all database tools.
"""

import json
import os

from dotenv import load_dotenv
from fastmcp import Context, FastMCP
from mcp_server.db import Database
from mcp_server.tools import (
    execute_sql_query,
    get_semantic_definitions,
    get_table_schema,
    list_tables,
    search_relevant_tables,
)

# Load environment variables
load_dotenv()

# Initialize the MCP Server
mcp = FastMCP("Text 2 SQL Agent")


def extract_tenant_id(ctx: Context) -> int | None:
    """Extract tenant_id from MCP request context.

    Priority:
    1. HTTP header 'X-Tenant-ID' (production)
    2. MCP initialization params (if available)
    3. Environment variable DEFAULT_TENANT_ID (local dev)

    Args:
        ctx: FastMCP Context object

    Returns:
        tenant_id as integer, or None if not found
    """
    # Try HTTP headers (SSE transport)
    if hasattr(ctx, "request_context") and ctx.request_context:
        headers = getattr(ctx.request_context, "headers", {})
        tenant_id_str = headers.get("x-tenant-id") or headers.get("X-Tenant-ID")
        if tenant_id_str:
            try:
                return int(tenant_id_str)
            except ValueError:
                pass

    # Try MCP initialization params (if available)
    if hasattr(ctx, "params") and ctx.params:
        tenant_id = ctx.params.get("tenant_id")
        if tenant_id:
            try:
                return int(tenant_id)
            except ValueError:
                pass

    # Fallback for local dev / stdio transport
    default_tenant = os.getenv("DEFAULT_TENANT_ID")
    if default_tenant:
        try:
            return int(default_tenant)
        except ValueError:
            pass

    return None


# Register tools
@mcp.tool()
async def list_tables_tool(search_term: str = None, ctx: Context = None) -> str:
    """List available tables in the database. Use this to discover table names."""
    tenant_id = extract_tenant_id(ctx) if ctx else None
    return await list_tables(search_term, tenant_id)


@mcp.tool()
async def get_table_schema_tool(table_names: list[str], ctx: Context = None) -> str:
    """Retrieve the schema (columns, data types, foreign keys) for a list of tables."""
    tenant_id = extract_tenant_id(ctx) if ctx else None
    return await get_table_schema(table_names, tenant_id)


@mcp.tool()
async def execute_sql_query_tool(sql_query: str, ctx: Context = None) -> str:
    """Execute a valid SQL SELECT statement and return the result as JSON.

    Strictly read-only. Requires tenant context for RLS enforcement.
    """
    tenant_id = extract_tenant_id(ctx) if ctx else None

    if tenant_id is None:
        error_msg = (
            "Unauthorized. No Tenant ID context found. "
            "Set X-Tenant-ID header or DEFAULT_TENANT_ID env var."
        )
        return json.dumps({"error": error_msg})

    return await execute_sql_query(sql_query, tenant_id)


@mcp.tool()
async def get_semantic_definitions_tool(terms: list[str], ctx: Context = None) -> str:
    """Retrieve business metric definitions from the semantic layer."""
    tenant_id = extract_tenant_id(ctx) if ctx else None
    return await get_semantic_definitions(terms, tenant_id)


@mcp.tool()
async def search_relevant_tables_tool(user_query: str, limit: int = 5, ctx: Context = None) -> str:
    """Search for tables relevant to a natural language query using semantic similarity."""
    tenant_id = extract_tenant_id(ctx) if ctx else None
    return await search_relevant_tables(user_query, limit, tenant_id)


if __name__ == "__main__":
    import asyncio
    import signal
    import sys

    # Initialize database connection pool before starting server
    async def init_database():
        """Initialize database connection pool and index schema."""
        await Database.init()

        # Check if schema_embeddings table is empty
        from mcp_server.indexer import index_all_tables

        async with Database.get_connection() as conn:
            count = await conn.fetchval("SELECT COUNT(*) FROM public.schema_embeddings")
            if count == 0:
                print("Schema embeddings table is empty. Starting indexing...")
                await index_all_tables()
            else:
                print(f"Schema already indexed ({count} tables)")

    # Initialize database
    asyncio.run(init_database())

    # Register cleanup handler for graceful shutdown
    def cleanup(signum, frame):
        """Cleanup database connections on shutdown."""
        asyncio.run(Database.close())
        sys.exit(0)

    signal.signal(signal.SIGINT, cleanup)
    signal.signal(signal.SIGTERM, cleanup)

    # Run via SSE for Docker compatibility.
    # Host must be 0.0.0.0 to be accessible from outside the container.
    mcp.run(transport="sse", host="0.0.0.0", port=8000)
