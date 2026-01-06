"""RAG Engine for semantic schema retrieval."""

from typing import Optional

from fastembed import TextEmbedding
from src.db import Database


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
    def embed_text(cls, text: str) -> list[float]:
        """
        Generate embedding vector for a text string.

        Args:
            text: Input text to embed.

        Returns:
            List of 384 float values representing the embedding.
        """
        model = cls._get_model()
        # fastembed returns an iterator, convert to list
        embedding = list(model.embed([text]))[0]
        return embedding.tolist()

    @classmethod
    def embed_batch(cls, texts: list[str]) -> list[list[float]]:
        """
        Generate embeddings for multiple texts efficiently.

        Args:
            texts: List of input texts to embed.

        Returns:
            List of embedding vectors.
        """
        model = cls._get_model()
        embeddings = list(model.embed(texts))
        return [emb.tolist() for emb in embeddings]


def format_vector_for_postgres(embedding: list[float]) -> str:
    """
    Format Python list as PostgreSQL vector string.

    Args:
        embedding: List of float values.

    Returns:
        String in format '[1.0, 2.0, 3.0]' for PostgreSQL.
    """
    return "[" + ",".join(str(float(x)) for x in embedding) + "]"


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


async def search_similar_tables(
    query_embedding: list[float],
    limit: int = 5,
) -> list[dict]:
    """
    Search for similar tables using cosine distance.

    Args:
        query_embedding: Embedding vector of the user query.
        limit: Maximum number of results to return.

    Returns:
        List of dicts with 'table_name', 'schema_text', 'distance'.
    """
    conn = await Database.get_connection()
    try:
        # Format embedding for PostgreSQL
        pg_vector = format_vector_for_postgres(query_embedding)

        # Use cosine distance operator (<=>)
        # Lower distance = more similar
        query = """
            SELECT
                table_name,
                schema_text,
                (embedding <=> $1::vector) as distance
            FROM public.schema_embeddings
            ORDER BY distance ASC
            LIMIT $2
        """

        rows = await conn.fetch(query, pg_vector, limit)
        return [
            {
                "table_name": row["table_name"],
                "schema_text": row["schema_text"],
                "distance": float(row["distance"]),
            }
            for row in rows
        ]
    finally:
        await Database.release_connection(conn)
