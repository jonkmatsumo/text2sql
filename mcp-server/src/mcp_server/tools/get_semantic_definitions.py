"""MCP tool: get_semantic_definitions - Retrieve business metric definitions."""

import json
from typing import Optional

from dal.database import Database

TOOL_NAME = "get_semantic_definitions"


async def handler(terms: list[str], tenant_id: Optional[int] = None) -> str:
    """Retrieve business metric definitions from the semantic layer.

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
