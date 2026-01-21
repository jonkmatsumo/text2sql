from typing import List, Protocol, runtime_checkable

from schema.rag import SchemaEmbedding


@runtime_checkable
class SchemaStore(Protocol):
    """Protocol for accessing table schema embeddings.

    Abstracts the storage of schema embeddings (Postgres, localized file, etc.).
    """

    async def fetch_schema_embeddings(self) -> List[SchemaEmbedding]:
        """Fetch all schema embeddings.

        Returns:
            List of canonical SchemaEmbedding objects.
        """
        ...

    async def save_schema_embedding(self, embedding: SchemaEmbedding) -> None:
        """Save (upsert) a schema embedding.

        Args:
            embedding: The schema embedding to save.
        """
        ...
