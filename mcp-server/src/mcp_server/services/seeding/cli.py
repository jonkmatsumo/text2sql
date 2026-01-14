"""Dedicated Seeder CLI.

This script runs in a separate initial container to seed the database
before the main MCP server starts.
"""

import asyncio
import sys
from pathlib import Path

from mcp_server.config.database import Database
from mcp_server.dal.factory import get_schema_introspector
from mcp_server.services.ingestion.graph_hydrator import GraphHydrator
from mcp_server.services.rag.engine import (
    RagEngine,
    format_vector_for_postgres,
    generate_schema_document,
)
from mcp_server.services.registry import RegistryService
from mcp_server.services.seeding.loader import load_from_directory, load_table_summaries


async def _ingest_graph_schema():
    """Ingest schema into Memgraph using SchemaIntrospector."""
    print("Ingesting graph schema...")
    try:
        # Get introspector (Postgres connection assumed via env vars)
        introspector = get_schema_introspector()

        # Hydrate
        hydrator = GraphHydrator()
        try:
            # Run async hydration
            await hydrator.hydrate_schema(introspector)
            print("✓ Graph schema ingestion complete.")

            from mcp_server.services.ingestion.vector_index_ddl import (
                ensure_table_embedding_hnsw_index,
            )

            try:
                ensure_table_embedding_hnsw_index(hydrator.store)
            except Exception as e:
                # Log but do not block startup for Phase 1
                # (unless strictness required in later phases)
                print(f"⚠ Failed to ensure vector index: {e}")

        finally:
            hydrator.close()

    except Exception as e:
        print(f"Error ingesting graph schema: {e}")


async def _process_seed_data(base_path: Path):
    """Load and register query examples into the unified registry."""
    items = load_from_directory(base_path)
    if not items:
        print("No seed files found.")
        return

    print(f"Processing {len(items)} items from {base_path}...")
    for item in items:
        # Register in Unified Registry with both 'example' and 'golden' roles
        await RegistryService.register_pair(
            question=item["question"],
            sql_query=item["query"],
            tenant_id=item.get("tenant_id", 1),
            roles=["example", "golden"],
            status="verified",
            metadata={
                "category": item.get("category"),
                "difficulty": item.get("difficulty", "medium"),
                "expected_row_count": item.get("expected_row_count"),
            },
        )
    print(f"✓ Registered {len(items)} items in Unified Registry.")


async def main():
    """Run the seeding process."""
    print("Starting Seeder Service...")

    from mcp_server.config.control_plane import ControlPlaneDatabase

    # Reuse the MCP Server's DB logic (inits both if configured)
    await Database.init()

    try:
        # We need two connections:
        # 1. Main DB (Pagila) for reading schema info
        # 2. Control DB for writing embeddings/examples

        # Primary/Main Connection
        async with Database.get_connection() as conn_main:
            from common.config.env import get_env_str

            print(
                f"✓ Main DB connection established: "
                f"{get_env_str('POSTGRES_USER')}@{get_env_str('DB_HOST')}/"
                f"{get_env_str('POSTGRES_DB')}"
            )

            # Control Connection (Direct Write)
            # If Control Plane is maintained by Database.init(), we can request it.
            # If valid, use it. Else fallback to main (legacy/single-db).
            if ControlPlaneDatabase.is_configured():
                ctx = ControlPlaneDatabase.get_direct_connection()
            else:
                print("⚠ Control Plane not configured. Using Main DB for control tables.")
                # Reuse main pool (but we need a new context manager or just use conn_main)
                # Since we need async context manager, we need a helper or
                # just Database.get_connection() again.
                ctx = Database.get_connection()

            async with ctx as conn_control:
                print("✓ Control DB connection established.")

                # 1. Seed Few-Shot Examples & Golden Dataset (Write to Registry via RegistryService)
                await _process_seed_data(Path("/app/queries"))

                # 2. Seed Table Summaries (Read Main, Write Control)
                await _seed_table_summaries(conn_main, conn_control, Path("/app/queries"))

                # 3. Graph Ingestion (Schema Parsing from Main -> Memgraph)
                # Uses DAL factory which uses Database/PostgresSchemaStore.
                # PostgresSchemaStore handles routing internally now.
                await _ingest_graph_schema()

            print("✓ Successfully processed all seed operations.")

    except Exception as e:
        print(f"Error during seeding: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)
    finally:
        await Database.close()
        print("✓ Database connection pools closed")


async def _seed_table_summaries(conn_read: any, conn_write: any, base_path: Path):
    """Load and index table summaries."""
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

        # Fetch authoritative columns from DB (Read Main)
        cols_query = """
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_name = $1 AND table_schema = 'public'
            ORDER BY ordinal_position
        """
        columns = await conn_read.fetch(cols_query, table_name)

        # If columns not found, skip or warn
        if not columns:
            continue

        # Get foreign keys (Read Main)
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
        foreign_keys = await conn_read.fetch(fk_query, table_name)

        # Generate enriched schema document
        schema_text = generate_schema_document(
            table_name,
            [dict(col) for col in columns],
            [dict(fk) for fk in foreign_keys] if foreign_keys else None,
            table_comment=summary,
        )

        # Generate embedding
        embedding = RagEngine.embed_text(schema_text)
        pg_vector = format_vector_for_postgres(embedding)

        # Upsert (Write Control)
        upsert_query = """
            INSERT INTO public.schema_embeddings (table_name, schema_text, embedding)
            VALUES ($1, $2, $3::vector)
            ON CONFLICT (table_name)
            DO UPDATE SET
                schema_text = EXCLUDED.schema_text,
                embedding = EXCLUDED.embedding,
                updated_at = CURRENT_TIMESTAMP
        """
        await conn_write.execute(upsert_query, table_name, schema_text, pg_vector)
        print(f"  ✓ Indexed Summary: {table_name}")


if __name__ == "__main__":
    asyncio.run(main())
