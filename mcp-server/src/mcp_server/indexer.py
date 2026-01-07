"""Schema indexing service for RAG."""

from mcp_server.db import Database
from mcp_server.rag import RagEngine, format_vector_for_postgres, generate_schema_document


async def index_all_tables():
    """
    Scan database schema and create embeddings for all tables.

    This function:
    1. Queries information_schema for all tables
    2. For each table, retrieves columns and foreign keys
    3. Generates enriched schema documents
    4. Creates embeddings
    5. Inserts/updates schema_embeddings table
    """
    # Schema indexing is global (not tenant-scoped), so no tenant_id needed
    async with Database.get_connection() as conn:
        # Get all tables in public schema
        tables_query = """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
            AND table_type = 'BASE TABLE'
            ORDER BY table_name
        """
        tables = await conn.fetch(tables_query)

        print(f"Indexing {len(tables)} tables...")

        for table_row in tables:
            table_name = table_row["table_name"]

            # Get columns
            cols_query = """
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns
                WHERE table_name = $1 AND table_schema = 'public'
                ORDER BY ordinal_position
            """
            columns = await conn.fetch(cols_query, table_name)

            # Get foreign keys
            fk_query = """
                SELECT
                    kcu.column_name,
                    ccu.table_name AS foreign_table_name,
                    ccu.column_name AS foreign_column_name
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

            # Generate schema document
            schema_text = generate_schema_document(
                table_name,
                [dict(col) for col in columns],
                [dict(fk) for fk in foreign_keys] if foreign_keys else None,
            )

            # Generate embedding
            embedding = RagEngine.embed_text(schema_text)
            pg_vector = format_vector_for_postgres(embedding)

            # Insert or update
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
            print(f"  ✓ Indexed: {table_name}")

        print(f"✓ Schema indexing complete: {len(tables)} tables indexed")
