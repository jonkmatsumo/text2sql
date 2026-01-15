"""RAG Engine for semantic schema retrieval."""

import asyncio
from typing import Optional

import numpy as np
from fastembed import TextEmbedding

from common.interfaces.vector_index import VectorIndex
from ingestion.vector_indexes.factory import create_vector_index

from .schema_loader import SchemaLoader


class RagEngine:
    """Manages embedding model lifecycle and vector generation."""

    _model: Optional[TextEmbedding] = None

    @classmethod
    def _get_model(cls) -> TextEmbedding:
        """Lazy load the embedding model."""
        if cls._model is None:
            # BAAI/bge-small-en-v1.5: 384 dimensions, optimized for retrieval
            cls._model = TextEmbedding(model_name="BAAI/bge-small-en-v1.5")
            print("✓ Embedding model loaded: BAAI/bge-small-en-v1.5")
        return cls._model

    @classmethod
    async def embed_text(cls, text: str) -> list[float]:
        """
        Generate embedding vector for a text string.

        Args:
            text: Input text to embed.

        Returns:
            List of 384 float values representing the embedding.
        """

        def _embed():
            model = cls._get_model()
            # fastembed returns an iterator, convert to list
            embedding = list(model.embed([text]))[0]
            return embedding.tolist()

        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, _embed)

    @classmethod
    async def embed_batch(cls, texts: list[str]) -> list[list[float]]:
        """
        Generate embeddings for multiple texts efficiently.

        Args:
            texts: List of input texts to embed.

        Returns:
            List of embedding vectors.
        """

        def _embed_batch():
            model = cls._get_model()
            embeddings = list(model.embed(texts))
            return [emb.tolist() for emb in embeddings]

        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, _embed_batch)


def generate_schema_document(
    table_name: str,
    columns: list[dict],
    foreign_keys: list[dict] = None,
    table_comment: str = None,
) -> str:
    """
    Generate enriched schema document for embedding.

    Enhanced version with table comments and semantic descriptions.

    Args:
        table_name: Name of the table.
        columns: List of column dicts with 'column_name', 'data_type', 'is_nullable'.
        foreign_keys: Optional list of FK dicts with 'column_name', 'foreign_table_name'.
        table_comment: Optional table comment/description.

    Returns:
        Concatenated text description suitable for semantic search.
    """
    doc_parts = []

    # Table name and description
    if table_comment:
        doc_parts.append(f"Table {table_name}: {table_comment}")
    else:
        doc_parts.append(f"Table: {table_name}")

    # Column descriptions with types
    col_descriptions = []
    for col in columns:
        col_name = col["column_name"]
        col_type = col["data_type"]
        nullable = "nullable" if col["is_nullable"] == "YES" else "required"

        # Add semantic hints based on column name patterns
        semantic_hint = ""
        col_lower = col_name.lower()
        if "id" in col_lower:
            semantic_hint = "identifier"
        elif "date" in col_lower or "time" in col_lower:
            semantic_hint = "timestamp"
        elif "amount" in col_lower or "price" in col_lower:
            semantic_hint = "monetary value"

        if semantic_hint:
            col_descriptions.append(f"{col_name} ({col_type}, {semantic_hint}, {nullable})")
        else:
            col_descriptions.append(f"{col_name} ({col_type}, {nullable})")

    doc_parts.append("Columns: " + ", ".join(col_descriptions))

    # Foreign key relationships
    if foreign_keys:
        fk_descriptions = []
        for fk in foreign_keys:
            fk_descriptions.append(f"{fk['column_name']} links to {fk['foreign_table_name']}")
        doc_parts.append("Relationships: " + ", ".join(fk_descriptions))

    return ". ".join(doc_parts) + "."


def format_vector_for_postgres(embedding: list[float]) -> str:
    """
    Format Python list as PostgreSQL vector string.

    Args:
        embedding: List of float values.

    Returns:
        String in format '[1.0, 2.0, 3.0]' for PostgreSQL.
    """
    return "[" + ",".join(str(float(x)) for x in embedding) + "]"


_schema_index: Optional[VectorIndex] = None


async def _get_schema_index() -> VectorIndex:
    """
    Get or create the schema vector index.

    Implements lazy loading to avoid blocking server startup.
    The index is populated from the database using SchemaLoader.
    """
    global _schema_index
    if _schema_index is None:
        # Create persistent HNSW index (in-memory, backed by DB via loader)
        # Using 384 dimensions for BGE-small
        _schema_index = create_vector_index(dim=384)
        print("✓ Initialized new Schema Vector Index")

        # Load examples from DB
        try:
            loader = SchemaLoader()
            await loader.load_schema_embeddings(_schema_index)
        except Exception as e:
            print(f"Error loading schemas: {e}")
            raise e

    return _schema_index


async def reload_schema_index() -> None:
    """Force reload the schema index from the database."""
    global _schema_index
    _schema_index = None
    await _get_schema_index()


async def search_similar_tables(
    query_embedding: list[float],
    limit: int = 5,
    tenant_id: Optional[int] = None,
) -> list[dict]:
    """
    Search for similar tables using in-memory VectorIndex.

    Args:
        query_embedding: Embedding vector of the user query.
        limit: Maximum number of results to return.
        tenant_id: Optional tenant identifier (not required for schema queries).

    Returns:
        List of dicts with 'table_name', 'schema_text', 'distance'.
    """
    index = await _get_schema_index()

    query_vector = np.array(query_embedding, dtype=np.float32)
    results = index.search(query_vector, k=limit)

    structured_results = []

    for res in results:
        # SchemaLoader stores metadata with table_name and schema_text
        metadata = res.metadata or {}

        structured_results.append(
            {
                "table_name": str(res.id),  # We used table_name as ID
                "schema_text": metadata.get("schema_text", ""),
                "distance": 1.0 - res.score,  # Convert similarity back to distance for compat
            }
        )

    return structured_results
