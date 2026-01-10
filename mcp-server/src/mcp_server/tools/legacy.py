"""MCP tool implementations for database operations."""

import json
import re
from typing import Optional

import asyncpg
from mcp_server.config.database import Database
from mcp_server.rag import RagEngine, search_similar_tables


async def list_tables(search_term: Optional[str] = None, tenant_id: Optional[int] = None) -> str:
    """
    List available tables in the database. Use this to discover table names.

    Args:
        search_term: Optional fuzzy search string to filter table names (e.g. 'pay' -> 'payment').
        tenant_id: Optional tenant identifier (not required for schema queries).

    Returns:
        JSON array of table names as strings.
    """
    store = Database.get_metadata_store()
    tables = await store.list_tables()

    if search_term:
        search_term = search_term.lower()
        tables = [t for t in tables if search_term in t.lower()]

    return json.dumps(tables, separators=(",", ":"))


async def get_table_schema(table_names: list[str], tenant_id: Optional[int] = None) -> str:
    """
    Retrieve the schema (columns, data types, foreign keys) for a list of tables.

    Args:
        table_names: A list of exact table names (e.g. ['film', 'actor']).
        tenant_id: Optional tenant identifier (not required for schema queries).

    Returns:
        Markdown-formatted schema documentation.
    """
    store = Database.get_metadata_store()
    schema_list = []

    for table in table_names:
        try:
            # MetadataStore returns JSON string for a single table
            definition_json = await store.get_table_definition(table)
            definition = json.loads(definition_json)
            schema_list.append(definition)
        except Exception:
            # Silently skip tables that error (e.g. don't exist), consistent with legacy behavior?
            # Legacy ran one query per table, but mostly continued.
            continue

    return json.dumps(schema_list, separators=(",", ":"))


async def execute_sql_query(
    sql_query: str, tenant_id: Optional[int] = None, params: Optional[list] = None
) -> str:
    """
    Execute a valid SQL SELECT statement and return the result as JSON.

    Strictly read-only. Returns error messages as strings for self-correction.
    Requires tenant_id for RLS enforcement.

    Args:
        sql_query: A SQL SELECT query string.
        tenant_id: Required tenant identifier for RLS enforcement.
        params: Optional list of bind parameters (e.g. for rewritten queries).

    Returns:
        JSON array of result rows, or error message as string.
    """
    # Require tenant_id for RLS enforcement
    if tenant_id is None:
        error_msg = (
            "Unauthorized. No Tenant ID context found. "
            "Set X-Tenant-ID header or DEFAULT_TENANT_ID env var."
        )
        return json.dumps({"error": error_msg})

    # 1. Application-Level Security Check (Pre-flight)
    # Reject mutative keywords to prevent injection attacks or accidental deletion
    forbidden_patterns = [
        r"(?i)\bDROP\b",
        r"(?i)\bDELETE\b",
        r"(?i)\bINSERT\b",
        r"(?i)\bUPDATE\b",
        r"(?i)\bALTER\b",
        r"(?i)\bGRANT\b",
        r"(?i)\bREVOKE\b",
        r"(?i)\bTRUNCATE\b",
        r"(?i)\bCREATE\b",
    ]

    for pattern in forbidden_patterns:
        if re.search(pattern, sql_query):
            return (
                f"Error: Query contains forbidden keyword matching '{pattern}'. "
                "Read-only access only."
            )

    async with Database.get_connection(tenant_id) as conn:
        try:
            # 2. Execution
            if params:
                rows = await conn.fetch(sql_query, *params)
            else:
                rows = await conn.fetch(sql_query)

            # 3. Serialization
            # Convert Record objects to dicts, handling non-serializable types
            result = [dict(row) for row in rows]

            # 4. Size Safety Valve
            if len(result) > 1000:
                error_msg = (
                    f"Result set too large ({len(result)} rows). "
                    "Please add a LIMIT clause to your query."
                )
                return json.dumps(
                    {
                        "error": error_msg,
                        "truncated_result": result[:1000],
                    },
                    default=str,
                )

            return json.dumps(
                result, default=str, separators=(",", ":")
            )  # default=str handles Date/Decimal types

        except asyncpg.PostgresError as e:
            # Return DB error as JSON object
            return json.dumps({"error": f"Database Error: {str(e)}"})
        except Exception as e:
            return json.dumps({"error": f"Execution Error: {str(e)}"})


async def get_semantic_definitions(terms: list[str], tenant_id: Optional[int] = None) -> str:
    """
    Retrieve business metric definitions from the semantic layer.

    Args:
        terms: List of term names to look up (e.g. ['High Value Customer', 'Churned']).
        tenant_id: Optional tenant identifier (not required for semantic definitions).

    Returns:
        JSON object mapping term names to their definitions and SQL logic.
    """
    if not terms:
        return json.dumps({})

    # Build parameterized query for multiple terms
    placeholders = ",".join([f"${i+1}" for i in range(len(terms))])
    query = f"""
        SELECT term_name, definition, sql_logic
        FROM public.semantic_definitions
        WHERE term_name = ANY(ARRAY[{placeholders}])
    """

    async with Database.get_connection(tenant_id) as conn:
        rows = await conn.fetch(query, *terms)

        result = {
            row["term_name"]: {
                "definition": row["definition"],
                "sql_logic": row["sql_logic"],
            }
            for row in rows
        }

        return json.dumps(result, separators=(",", ":"))


async def search_relevant_tables(
    user_query: str, limit: int = 5, tenant_id: Optional[int] = None
) -> str:
    """
    Search for tables relevant to a natural language query using semantic similarity.

    This tool solves the context window problem by returning only the most relevant
    table schemas instead of the entire database schema.

    Args:
        user_query: Natural language question (e.g., "Show me customer payments")
        limit: Maximum number of relevant tables to return (default: 5)
        tenant_id: Optional tenant identifier (not required for schema queries).

    Returns:
        Markdown-formatted string containing schema definitions of relevant tables.
    """
    # Generate embedding for user query
    query_embedding = RagEngine.embed_text(user_query)

    # Search for similar tables
    results = await search_similar_tables(query_embedding, limit=limit, tenant_id=tenant_id)

    structured_results = []
    introspector = Database.get_schema_introspector()

    for result in results:
        table_name = result["table_name"]

        # Use introspector to get columns (abstracted from SQL)
        try:
            table_def = await introspector.get_table_def(table_name)

            table_columns = [
                {
                    "name": col.name,
                    "type": col.data_type,
                    "required": not col.is_nullable,
                }
                for col in table_def.columns
            ]

            structured_results.append(
                {
                    "table_name": table_name,
                    "description": result[
                        "schema_text"
                    ],  # Using schema_text as description for now
                    "similarity": 1 - result["distance"],
                    "columns": table_columns,
                }
            )
        except Exception:
            # Skip tables that might be in index but missing in introspection (race condition?)
            continue

    return json.dumps(structured_results, separators=(",", ":"))
