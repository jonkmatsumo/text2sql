import json
from typing import List

from mcp_server.config.database import Database
from mcp_server.dal.interfaces.schema_store import SchemaStore
from mcp_server.dal.postgres.common import _format_vector
from mcp_server.models.rag.embedding import SchemaEmbedding


class PostgresSchemaStore(SchemaStore):
    """Postgres implementation of SchemaStore."""

    async def fetch_schema_embeddings(self) -> List[SchemaEmbedding]:
        """Fetch all schema embeddings from schema_embeddings table.

        Returns:
            List of canonical SchemaEmbedding objects.
        """
        query = """
            SELECT table_name, schema_text, embedding
            FROM public.schema_embeddings
            WHERE embedding IS NOT NULL
        """

        async with Database.get_connection() as conn:
            rows = await conn.fetch(query)

        results = []
        for row in rows:
            embedding_val = row["embedding"]
            if isinstance(embedding_val, str):
                vector = json.loads(embedding_val)
            else:
                vector = list(embedding_val)

            results.append(
                SchemaEmbedding(
                    table_name=row["table_name"],
                    schema_text=row["schema_text"],
                    embedding=vector,
                )
            )

        return results

    async def save_schema_embedding(self, embedding: SchemaEmbedding) -> None:
        """Save (upsert) a schema embedding.

        Args:
            embedding: The schema embedding to save.
        """
        pg_vector = _format_vector(embedding.embedding)

        query = """
            INSERT INTO public.schema_embeddings (table_name, schema_text, embedding)
            VALUES ($1, $2, $3::vector)
            ON CONFLICT (table_name)
            DO UPDATE SET
                schema_text = EXCLUDED.schema_text,
                embedding = EXCLUDED.embedding,
                updated_at = CURRENT_TIMESTAMP
        """

        async with Database.get_connection() as conn:
            await conn.execute(query, embedding.table_name, embedding.schema_text, pg_vector)
