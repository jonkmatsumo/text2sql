"""Dedicated Seeder CLI.

This script runs in a separate initial container to seed the database
before the main MCP server starts.
"""

import asyncio
import json
import os
import sys
from pathlib import Path

from mcp_server.config.database import Database
from mcp_server.dal.factory import get_retriever
from mcp_server.dal.ingestion.hydrator import GraphHydrator
from mcp_server.rag import RagEngine, format_vector_for_postgres
from mcp_server.seeding.loader import load_from_directory


async def _ingest_graph_schema():
    """Ingest schema into Memgraph using DataSchemaRetriever."""
    print("Ingesting graph schema...")
    try:
        # Get retriever (Postgres connection assumed via env vars)
        retriever = get_retriever()

        # Hydrate - use MEMGRAPH_URI from environment
        memgraph_uri = os.getenv("MEMGRAPH_URI", "bolt://localhost:7687")
        hydrator = GraphHydrator(uri=memgraph_uri)
        try:
            # Run blocking hydration code in executor if needed,
            # but for seeding script simplicity we can run it directly
            # as long as we accept it blocks the asyncio loop temporarily
            # (which is fine for a linear CLI script).
            hydrator.hydrate_schema(retriever)
            print("✓ Graph schema ingestion complete.")
        finally:
            hydrator.close()

    except Exception as e:
        print(f"Error ingesting graph schema: {e}")


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


async def _process_seed_data(conn: any, base_path: Path):
    """Load and upsert query examples."""
    items = load_from_directory(base_path)
    if not items:
        print("No seed files found.")
        return

    print(f"Processing {len(items)} items from {base_path}...")
    for item in items:
        await _upsert_sql_example(conn, item)
        await _upsert_golden_record(conn, item)


async def _seed_table_summaries(conn: any, base_path: Path):
    """Load and index table summaries."""
    from mcp_server.rag import RagEngine, format_vector_for_postgres, generate_schema_document
    from mcp_server.seeding.loader import load_table_summaries

    summaries = load_table_summaries(base_path)
    if not summaries:
        print("No table summaries found.")
        return

    print(f"Seeding {len(summaries)} table summaries...")
    for item in summaries:
        table_name = item.get("table_name")
        summary = item.get("summary")
        if not table_name:
            continue

        # Fetch authoritative columns from DB
        cols_query = """
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_name = $1 AND table_schema = 'public'
            ORDER BY ordinal_position
        """
        columns = await conn.fetch(cols_query, table_name)

        # If columns not found, skipping might be safer, but maybe the table is in another schema?
        # Assuming public schema for now as per Pagila.
        if not columns:
            # Just index the summary if table doesn't exist? No, better to skip or warn.
            # But for 'Identification' phase, we might want it even if table missing?
            # No, if table missing, we can't query it.
            # However, we should fetch FKs too.
            pass

        # Get foreign keys
        fk_query = """
            SELECT
                kcu.column_name,
                ccu.table_name AS foreign_table_name
            FROM information_schema.table_constraints AS tc
            JOIN information_schema.key_column_usage AS kcu
                ON tc.constraint_name = kcu.constraint_name
            JOIN information_schema.constraint_column_usage AS ccu
                ON ccu.constraint_name = tc.constraint_name
            WHERE tc.constraint_type = 'FOREIGN KEY'
                AND tc.table_name = $1
                AND tc.table_schema = 'public'
        """
        foreign_keys = await conn.fetch(fk_query, table_name)

        # Generate enriched schema document
        # We pass the summary as 'table_comment' to be embedded
        schema_text = generate_schema_document(
            table_name,
            [dict(col) for col in columns],
            [dict(fk) for fk in foreign_keys] if foreign_keys else None,
            table_comment=summary,
        )

        # Generate embedding
        embedding = RagEngine.embed_text(schema_text)
        pg_vector = format_vector_for_postgres(embedding)

        # Upsert
        upsert_query = """
            INSERT INTO public.schema_embeddings (table_name, schema_text, embedding)
            VALUES ($1, $2, $3::vector)
            ON CONFLICT (table_name)
            DO UPDATE SET
                schema_text = EXCLUDED.schema_text,
                embedding = EXCLUDED.embedding,
                updated_at = CURRENT_TIMESTAMP
        """
        await conn.execute(upsert_query, table_name, schema_text, pg_vector)
        print(f"  ✓ Indexed Summary: {table_name}")


async def main():
    """Run the seeding process."""
    print("Starting Seeder Service...")

    # Reuse the MCP Server's DB logic
    await Database.init()

    try:
        async with Database.get_connection() as conn:
            print(
                f"✓ Database connection pool established: "
                f"{os.getenv('POSTGRES_USER')}@{os.getenv('DB_HOST')}/{os.getenv('POSTGRES_DB')}"
            )

            # 1. Seed Few-Shot Examples & Golden Dataset
            await _process_seed_data(conn, Path("/app/queries"))

            # 2. Seed Table Summaries (Schema Context)
            await _seed_table_summaries(conn, Path("/app/queries"))

            # 3. Graph Ingestion (Schema Parsing)
            await _ingest_graph_schema()

            print("✓ Successfully processed all seed operations.")

    except Exception as e:
        print(f"Error during seeding: {e}")
        import traceback

        traceback.print_exc()
        # Non-zero exit code to restart container or signal failure
        sys.exit(1)
    finally:
        await Database.close()
        print("✓ Database connection pool closed")


if __name__ == "__main__":
    asyncio.run(main())
