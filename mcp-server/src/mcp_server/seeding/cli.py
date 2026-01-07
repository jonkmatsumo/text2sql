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


async def _upsert_sql_example(conn, item: dict):
    """Upsert item into sql_examples."""
    question = item["question"]
    sql = item["query"]

    # Compute embedding
    # TODO: Add batch support to RagEngine for faster seeding
    embedding = RagEngine.embed_text(question)
    pg_vector = format_vector_for_postgres(embedding)

    await conn.execute(
        """
        INSERT INTO sql_examples (question, sql_query, embedding)
        VALUES ($1, $2, $3)
        ON CONFLICT (question) DO UPDATE
        SET sql_query = EXCLUDED.sql_query,
            embedding = EXCLUDED.embedding,
            updated_at = NOW()
        """,
        question,
        sql,
        pg_vector,
    )


async def _upsert_golden_record(conn, item: dict):
    """Upsert item into golden_dataset."""
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
        ON CONFLICT (question) DO UPDATE
        SET ground_truth_sql = EXCLUDED.ground_truth_sql,
            expected_result = EXCLUDED.expected_result,
            expected_row_count = EXCLUDED.expected_row_count,
            category = EXCLUDED.category,
            difficulty = EXCLUDED.difficulty,
            tenant_id = EXCLUDED.tenant_id
        """,
        item["question"],
        item["query"],
        expected_result,
        item.get("expected_row_count"),
        item.get("category"),
        item.get("difficulty", "medium"),
        item.get("tenant_id", 1),
    )


async def main():
    """Run the seeding process."""
    print("Starting Seeder Service...")
    await Database.init()

    try:
        base_path = Path("/app/seeds")
        items = load_from_directory(base_path)

        if not items:
            print("No seed files found.")
            return

        print(f"Processing {len(items)} items from {base_path}...")

        async with Database.get_connection() as conn:
            for item in items:
                # 1. Seed SQL Example
                await _upsert_sql_example(conn, item)

                # 2. Seed Golden Dataset
                await _upsert_golden_record(conn, item)

        print(f"✓ Successfully processed {len(items)} items into both tables.")

    except Exception as e:
        print(f"Seeding failed: {e}")
        raise
    finally:
        await Database.close()


if __name__ == "__main__":
    asyncio.run(main())
