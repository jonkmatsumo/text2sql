"""Generate embeddings for SQL examples (Phase 2 seeding).

This module is responsible for generating vector embeddings for sql_examples
rows that are missing them. The static data (question, query, summary) is
inserted by database init scripts (Phase 1).
"""

from mcp_server.db import Database
from mcp_server.rag import RagEngine, format_vector_for_postgres


async def generate_missing_embeddings() -> int:
    """Generate embeddings for sql_examples rows with NULL embedding.

    Uses the question text as the basis for embedding, enabling semantic
    similarity matching when users ask questions.

    Returns:
        Number of embeddings generated.
    """
    # Find rows missing embeddings
    async with Database.get_connection() as conn:
        rows = await conn.fetch(
            """
            SELECT id, question, summary
            FROM sql_examples
            WHERE embedding IS NULL
            """
        )

    if not rows:
        return 0

    print(f"Generating embeddings for {len(rows)} examples...")

    generated = 0
    for row in rows:
        # Use summary (which equals question) for embedding
        text_to_embed = row["summary"] or row["question"]
        embedding = RagEngine.embed_text(text_to_embed)
        pg_vector = format_vector_for_postgres(embedding)

        async with Database.get_connection() as conn:
            await conn.execute(
                """
                UPDATE sql_examples
                SET embedding = $1
                WHERE id = $2
                """,
                pg_vector,
                row["id"],
            )
        generated += 1

    print(f"✓ Generated {generated} embeddings")
    return generated
