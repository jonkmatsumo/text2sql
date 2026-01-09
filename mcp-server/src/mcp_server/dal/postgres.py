from typing import List, Optional

from mcp_server.config.database import Database
from mcp_server.dal.interfaces import (
    CacheStore,
    ExampleStore,
    MetadataStore,
    SchemaIntrospector,
    SchemaStore,
)
from mcp_server.models.dal_types import (
    CacheLookupResult,
    ColumnDef,
    Example,
    ForeignKeyDef,
    SchemaEmbedding,
    TableDef,
)


def _format_vector(embedding: List[float]) -> str:
    """Format Python list as PostgreSQL vector string."""
    return "[" + ",".join(str(float(x)) for x in embedding) + "]"


class PgSemanticCache(CacheStore):
    """PostgreSQL implementation of Semantic Cache using pgvector."""

    async def lookup(
        self,
        query_embedding: List[float],
        tenant_id: int,
        threshold: float = 0.95,
    ) -> Optional[CacheLookupResult]:
        """Lookup a cached result by embedding similarity."""
        pg_vector = _format_vector(query_embedding)

        # 1 - distance = similarity (cosine)
        query = """
            SELECT
                cache_id,
                generated_sql,
                (1 - (query_embedding <=> $1)) as similarity
            FROM semantic_cache
            WHERE tenant_id = $2
            AND query_embedding IS NOT NULL
            AND (1 - (query_embedding <=> $1)) >= $3
            ORDER BY similarity DESC
            LIMIT 1
        """

        async with Database.get_connection(tenant_id) as conn:
            row = await conn.fetchrow(query, pg_vector, tenant_id, threshold)

        if row:
            return CacheLookupResult(
                cache_id=str(row["cache_id"]),
                value=row["generated_sql"],
                similarity=row["similarity"],
            )
        return None

    async def record_hit(self, cache_id: str, tenant_id: int) -> None:
        """Record a cache hit (fire-and-forget)."""
        try:
            # cache_id stored as int in Postgres
            id_val = int(cache_id)
        except ValueError:
            # Handle string IDs if we ever move to UUIDs, but for now schema is int
            return

        query = """
            UPDATE semantic_cache
            SET hit_count = hit_count + 1,
                last_accessed_at = NOW()
            WHERE cache_id = $1 AND tenant_id = $2
        """
        async with Database.get_connection(tenant_id) as conn:
            await conn.execute(query, id_val, tenant_id)

    async def store(
        self,
        user_query: str,
        generated_sql: str,
        query_embedding: List[float],
        tenant_id: int,
    ) -> None:
        """Store a new cache entry."""
        pg_vector = _format_vector(query_embedding)

        query = """
            INSERT INTO semantic_cache (tenant_id, user_query, query_embedding, generated_sql)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT DO NOTHING
        """
        async with Database.get_connection(tenant_id) as conn:
            await conn.execute(query, tenant_id, user_query, pg_vector, generated_sql)


class PostgresExampleStore(ExampleStore):
    """Postgres implementation of ExampleStore."""

    async def fetch_all_examples(self) -> List[Example]:
        """Fetch all examples from sql_examples table.

        Returns:
            List of canonical Example objects.
        """
        import json

        query = """
            SELECT id, question, sql_query, embedding
            FROM sql_examples
            WHERE embedding IS NOT NULL
        """

        async with Database.get_connection() as conn:
            rows = await conn.fetch(query)

        examples = []
        for row in rows:
            embedding_val = row["embedding"]
            if isinstance(embedding_val, str):
                vector = json.loads(embedding_val)
            else:
                vector = list(embedding_val)

            examples.append(
                Example(
                    id=row["id"],
                    question=row["question"],
                    sql_query=row["sql_query"],
                    embedding=vector,
                )
            )

        return examples


class PostgresSchemaStore(SchemaStore):
    """Postgres implementation of SchemaStore."""

    async def fetch_schema_embeddings(self) -> List[SchemaEmbedding]:
        """Fetch all schema embeddings from schema_embeddings table.

        Returns:
            List of canonical SchemaEmbedding objects.
        """
        import json

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


class PostgresSchemaIntrospector(SchemaIntrospector):
    """Postgres implementation of SchemaIntrospector using information_schema."""

    async def list_table_names(self, schema: str = "public") -> List[str]:
        """List all table names in the specified schema."""
        query = """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = $1
            AND table_type = 'BASE TABLE'
            ORDER BY table_name
        """

        async with Database.get_connection() as conn:
            rows = await conn.fetch(query, schema)

        return [row["table_name"] for row in rows]

    async def get_table_def(self, table_name: str, schema: str = "public") -> TableDef:
        """Get the full definition of a table (columns, FKs)."""
        async with Database.get_connection() as conn:
            # Columns
            cols_query = """
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns
                WHERE table_name = $1 AND table_schema = $2
                ORDER BY ordinal_position
            """
            col_rows = await conn.fetch(cols_query, table_name, schema)

            # Foreign Keys
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
                    AND tc.table_schema = $2
            """
            fk_rows = await conn.fetch(fk_query, table_name, schema)

        columns = [
            ColumnDef(
                name=row["column_name"],
                data_type=row["data_type"],
                is_nullable=(row["is_nullable"] == "YES"),
            )
            for row in col_rows
        ]

        fks = [
            ForeignKeyDef(
                column_name=row["column_name"],
                foreign_table_name=row["foreign_table_name"],
                foreign_column_name=row["foreign_column_name"],
            )
            for row in fk_rows
        ]
        return TableDef(name=table_name, columns=columns, foreign_keys=fks)


class PostgresMetadataStore(MetadataStore):
    """Postgres implementation of MetadataStore.

    Uses SchemaIntrospector to get structured data and formats it for tool output.
    """

    def __init__(self):
        """Initialize the metadata store with a schema introspector."""
        self._introspector = PostgresSchemaIntrospector()

    async def list_tables(self, schema: str = "public") -> List[str]:
        """List all available tables."""
        return await self._introspector.list_table_names(schema)

    async def get_table_definition(self, table_name: str) -> str:
        """Get the table schema formatted as the legacy tool expects (JSON)."""
        import json

        table_def = await self._introspector.get_table_def(table_name)

        # Format to match legacy.py output methodology
        columns_data = [
            {
                "name": col.name,
                "type": col.data_type,
                "nullable": col.is_nullable,
            }
            for col in table_def.columns
        ]

        foreign_keys = [
            {
                "column": fk.column_name,
                "foreign_table": fk.foreign_table_name,
                "foreign_column": fk.foreign_column_name,
            }
            for fk in table_def.foreign_keys
        ]

        definition = {
            "table_name": table_name,
            "columns": columns_data,
            "foreign_keys": foreign_keys,
        }

        # Return as JSON string just like the legacy tool did (but singular)
        return json.dumps(definition)
