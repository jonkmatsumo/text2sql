"""MCP Server entrypoint for Text 2 SQL Agent.

This module initializes the FastMCP server and registers all database tools.
"""

from dotenv import load_dotenv
from fastmcp import FastMCP
from src.db import Database
from src.tools import (
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


# Register tools
@mcp.tool()
async def list_tables_tool(search_term: str = None) -> str:
    """List available tables in the database. Use this to discover table names."""
    return await list_tables(search_term)


@mcp.tool()
async def get_table_schema_tool(table_names: list[str]) -> str:
    """Retrieve the schema (columns, data types, foreign keys) for a list of tables."""
    return await get_table_schema(table_names)


@mcp.tool()
async def execute_sql_query_tool(sql_query: str) -> str:
    """Execute a valid SQL SELECT statement and return the result as JSON. Strictly read-only."""
    return await execute_sql_query(sql_query)


@mcp.tool()
async def get_semantic_definitions_tool(terms: list[str]) -> str:
    """Retrieve business metric definitions from the semantic layer."""
    return await get_semantic_definitions(terms)


@mcp.tool()
async def search_relevant_tables_tool(user_query: str, limit: int = 5) -> str:
    """Search for tables relevant to a natural language query using semantic similarity."""
    return await search_relevant_tables(user_query, limit)


if __name__ == "__main__":
    import asyncio
    import signal
    import sys

    # Initialize database connection pool before starting server
    async def init_database():
        """Initialize database connection pool and index schema."""
        await Database.init()

        # Check if schema_embeddings table is empty
        from src.indexer import index_all_tables

        conn = await Database.get_connection()
        try:
            count = await conn.fetchval("SELECT COUNT(*) FROM public.schema_embeddings")
            if count == 0:
                print("Schema embeddings table is empty. Starting indexing...")
                await index_all_tables()
            else:
                print(f"Schema already indexed ({count} tables)")
        finally:
            await Database.release_connection(conn)

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
