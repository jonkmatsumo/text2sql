"""Dedicated Seeder CLI.

This script runs in a separate initial container to seed the database
before the main MCP server starts.
"""

import asyncio
import json
from pathlib import Path

from mcp_server.db import Database
from mcp_server.rag import RagEngine, format_vector_for_postgres
from mcp_server.seeding.loader import load_from_directory


async def seed_sql_examples(base_path: Path):
    """Seed sql_examples table with embeddings."""
    examples_path = base_path / "examples"
    items = load_from_directory(examples_path)

    if not items:
        return

    print(f"Seeding {len(items)} SQL examples...")

    # We can use batch embedding if RagEngine supports it, but loop is fine for seeded data
    # RagEngine.embed_text is single, let's allow it to init the model once.

    # Init DB
    async with Database.get_connection() as conn:
        for item in items:
            question = item["question"]
            sql = item["query"]

            # Compute embedding
            # TODO: Add batch support to RagEngine for faster seeding
            embedding = RagEngine.embed_text(question)
            pg_vector = format_vector_for_postgres(embedding)

            # Upsert
            # Matches on question text to avoid duplicates
            # Note: real prod might use a stable ID or hash
            await conn.execute(
                """
                INSERT INTO sql_examples (question, sql_query, embedding)
                VALUES ($1, $2, $3)
                ON CONFLICT DO NOTHING
                """,
                question,
                sql,
                pg_vector,
            )

    print("✓ SQL examples seeded")


async def seed_golden_dataset(base_path: Path):
    """Seed golden_dataset table."""
    golden_path = base_path / "golden"
    items = load_from_directory(golden_path)

    if not items:
        return

    print(f"Seeding {len(items)} golden test cases...")

    async with Database.get_connection() as conn:
        for item in items:
            # Handle optional fields
            expected_result = item.get("expected_result")
            if expected_result is not None:
                expected_result = json.dumps(expected_result)

            await conn.execute(
                """
                INSERT INTO golden_dataset (
                    question, ground_truth_sql, expected_result,
                    expected_row_count, category, difficulty, tenant_id
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                ON CONFLICT DO NOTHING
                """,
                item["question"],
                item["query"],  # 'query' in JSON maps to 'ground_truth_sql'
                expected_result,
                item.get("expected_row_count"),
                item.get("category"),
                item.get("difficulty", "medium"),
                item.get("tenant_id", 1),
            )

    print("✓ Golden dataset seeded")


async def main():
    """Run the seeding process."""
    print("Starting Seeder Service...")

    # Initialize DB connection
    await Database.init()

    try:
        base_path = Path("/app/seeds")

        # 1. Seed Examples (Few-Shot)
        await seed_sql_examples(base_path)

        # 2. Seed Golden Dataset
        await seed_golden_dataset(base_path)

        print("Seeding completed successfully.")

    except Exception as e:
        print(f"Seeding failed: {e}")
        raise
    finally:
        await Database.close()


if __name__ == "__main__":
    asyncio.run(main())
