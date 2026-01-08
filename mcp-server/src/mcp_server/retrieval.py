"""Retrieval module for dynamic few-shot learning."""

from typing import Optional

from mcp_server.db import Database
from mcp_server.rag import RagEngine, format_vector_for_postgres


async def get_relevant_examples(
    user_query: str,
    limit: int = 3,
    tenant_id: Optional[int] = None,
) -> str:
    """
    Retrieve few-shot examples similar to the user's query.

    Uses semantic similarity search on synthetic summaries to find relevant
    Golden SQL examples. This bridges the semantic gap between natural language
    questions and SQL code.

    Args:
        user_query: The user's natural language question
        limit: Maximum number of examples to retrieve (default: 3)
        tenant_id: Optional tenant ID (examples are usually global, but can be tenant-specific)

    Returns:
        Formatted string with examples, or empty string if none found
    """
    # 1. Embed the incoming question
    embedding = RagEngine.embed_text(user_query)
    pg_vector = format_vector_for_postgres(embedding)

    # 2. Search for similar examples using Cosine Distance (<=>)
    # We select the question, SQL, and summary to show the LLM
    query = """
        SELECT question, sql_query,
               (1 - (embedding <=> $1)) as similarity
        FROM sql_examples
        WHERE embedding IS NOT NULL
        ORDER BY embedding <=> $1
        LIMIT $2
    """

    # Note: Few-shot examples are usually global (shared knowledge),
    # so we might not need tenant_id here unless examples are tenant-specific.
    # We use a generic connection without tenant context.
    async with Database.get_connection() as conn:
        rows = await conn.fetch(query, pg_vector, limit)

    if not rows:
        return ""

    import json

    # 3. Format as a list of structured objects
    examples = [
        {
            "question": row["question"],
            "sql": row["sql_query"],
            "similarity": float(row["similarity"]),
        }
        for row in rows
    ]

    return json.dumps(examples, indent=2)
