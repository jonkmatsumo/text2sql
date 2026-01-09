"""Schema indexing service for RAG."""

import mcp_server.rag
from mcp_server.dal.types import SchemaEmbedding
from mcp_server.db import Database
from mcp_server.rag import RagEngine, generate_schema_document


async def index_all_tables():
    """
    Scan database schema and create embeddings for all tables.

    This function:
    1. Introspects database using SchemaIntrospector
    2. Generates enriched schema documents
    3. Creates embeddings
    4. Saves to SchemaStore
    """
    introspector = Database.get_schema_introspector()
    store = Database.get_schema_store()

    table_names = await introspector.list_table_names()
    print(f"Indexing {len(table_names)} tables...")

    for table_name in table_names:
        # Get full definition
        table_def = await introspector.get_table_def(table_name)

        # Convert canonical types to dicts for RagEngine
        # (RagEngine code stays as is, accepting generic dicts for flexibility)
        columns = [
            {
                "column_name": col.name,
                "data_type": col.data_type,
                "is_nullable": "YES" if col.is_nullable else "NO",
            }
            for col in table_def.columns
        ]

        foreign_keys = [
            {"column_name": fk.column_name, "foreign_table_name": fk.foreign_table_name}
            for fk in table_def.foreign_keys
        ]

        # Generate schema document
        schema_text = generate_schema_document(table_name, columns, foreign_keys)

        # Generate embedding
        embedding_vector = RagEngine.embed_text(schema_text)

        # Save to store
        schema_embedding = SchemaEmbedding(
            table_name=table_name, schema_text=schema_text, embedding=embedding_vector
        )

        await store.save_schema_embedding(schema_embedding)
        print(f"  ✓ Indexed: {table_name}")

    print(f"✓ Schema indexing complete: {len(table_names)} tables indexed")

    # Reload the in-memory vector index to reflect changes
    await mcp_server.rag.reload_schema_index()
    print("✓ Schema index reloaded")
