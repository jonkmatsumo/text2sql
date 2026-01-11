"""MCP Server entrypoint for Text 2 SQL Agent.

This module initializes the FastMCP server and registers all database tools.
"""

import json
import os
from contextlib import asynccontextmanager
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv
from fastmcp import Context, FastMCP
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
            else:
                print(f"Schema already indexed ({count} tables)")
    except Exception as e:
        print(f"Warning: Schema indexing skipped: {e}")

    yield  # Server runs here

    # Shutdown: Close database connection pool
    await Database.close()


# Initialize the MCP Server with lifespan
mcp = FastMCP("Text 2 SQL Agent", lifespan=lifespan)


def extract_tenant_id(ctx: Context) -> Optional[int]:
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
    return list_tables(search_term, tenant_id)


@mcp.tool()
async def get_table_schema_tool(table_names: list[str], ctx: Context = None) -> str:
    """
    Retrieve the schema (columns, data types, foreign keys) for a list of tables.

    Returns JSON list of table schemas.
    """
    tenant_id = extract_tenant_id(ctx) if ctx else None
    return get_table_schema(table_names, tenant_id)


@mcp.tool()
async def get_sample_data_tool(table_name: str, limit: int = 3, ctx: Context = None) -> str:
    """
    Get sample data rows from a table.

    Returns JSON list of sample rows.
    """
    tenant_id = extract_tenant_id(ctx) if ctx else None
    return get_sample_data(table_name, limit, tenant_id)


@mcp.tool()
async def execute_sql_query_tool(
    sql_query: str,
    tenant_id: Optional[int] = None,
    params: Optional[list] = None,
    ctx: Context = None,
) -> str:
    """Execute a valid SQL SELECT statement and return the result as JSON.

    Strictly read-only. Requires tenant context for RLS enforcement.
    """
    # Prefer explicit argument, fall back to context extraction
    final_tenant_id = (
        tenant_id if tenant_id is not None else (extract_tenant_id(ctx) if ctx else None)
    )

    if final_tenant_id is None:
        error_msg = (
            "Unauthorized. No Tenant ID context found. "
            "Set X-Tenant-ID header or DEFAULT_TENANT_ID env var."
        )
        return json.dumps({"error": error_msg}, separators=(",", ":"))

    return await execute_sql_query(sql_query, final_tenant_id, params)


@mcp.tool()
async def get_semantic_definitions_tool(terms: list[str], ctx: Context = None) -> str:
    """Retrieve business metric definitions from the semantic layer."""
    tenant_id = extract_tenant_id(ctx) if ctx else None
    return await get_semantic_definitions(terms, tenant_id)


@mcp.tool()
async def search_relevant_tables_tool(user_query: str, limit: int = 5, ctx: Context = None) -> str:
    """
    Search for tables relevant to a natural language query using semantic similarity.

    Returns JSON list of table schemas.
    """
    tenant_id = extract_tenant_id(ctx) if ctx else None
    return await search_relevant_tables(user_query, limit, tenant_id)


@mcp.tool()
async def resolve_ambiguity_tool(
    query: str, schema_context: List[Dict[str, Any]], ctx: Context = None
) -> str:
    """
    Resolve potential ambiguities in a user query against provided schema context.

    Returns JSON string with resolution status and bindings.
    """
    return await resolve_ambiguity(query, schema_context)


@mcp.tool()
async def get_few_shot_examples_tool(user_query: str, limit: int = 3, ctx: Context = None) -> str:
    """
    Retrieve relevant SQL examples for few-shot learning based on user query.

    Returns JSON list of examples.
    """
    tenant_id = extract_tenant_id(ctx) if ctx else None
    return await get_relevant_examples(user_query, limit, tenant_id)


@mcp.tool()
async def lookup_cache_tool(
    user_query: str, tenant_id: Optional[int] = None, ctx: Context = None
) -> str:
    """Check semantic cache for similar query. Returns cached SQL if similarity >= 0.90."""
    final_tenant_id = (
        tenant_id if tenant_id is not None else (extract_tenant_id(ctx) if ctx else None)
    )

    if not final_tenant_id:
        return json.dumps({"error": "Tenant ID required for cache lookup"})

    result = await lookup_cache(user_query, final_tenant_id)

    if result:
        return json.dumps(
            {
                "sql": result.value,
                "original_query": result.metadata.get("user_query"),
                "similarity": result.similarity,
                "metadata": result.metadata,
                "cache_id": result.cache_id,
            },
            separators=(",", ":"),
        )

    return json.dumps({"sql": None}, separators=(",", ":"))


@mcp.tool()
async def update_cache_tool(
    user_query: str, sql: str, tenant_id: Optional[int] = None, ctx: Context = None
) -> str:
    """Cache a successful SQL generation for future use."""
    final_tenant_id = (
        tenant_id if tenant_id is not None else (extract_tenant_id(ctx) if ctx else None)
    )

    if not final_tenant_id:
        return json.dumps({"error": "Tenant ID required for cache update"})
    await update_cache(user_query, sql, final_tenant_id)
    return json.dumps({"status": "cached"}, separators=(",", ":"))


@mcp.tool()
async def get_semantic_subgraph_tool(query: str, ctx: Context = None) -> str:
    """Retrieve relevant subgraph of tables/columns based on query.

    Use this to understand database structure.
    """
    tenant_id = extract_tenant_id(ctx) if ctx else None
    return await get_semantic_subgraph(query, tenant_id)


if __name__ == "__main__":
    # Run via streamable-http for better stability.
    # Host must be 0.0.0.0 to be accessible from outside the container.
    mcp.run(transport="streamable-http", host="0.0.0.0", port=8000)
