import logging
from typing import List, Optional

from neo4j import GraphDatabase
from openai import OpenAI

logger = logging.getLogger(__name__)


class EmbeddingService:
    """Service to generate vector embeddings using OpenAI."""

    def __init__(self, model: str = "text-embedding-3-small"):
        """Initialize OpenAI client."""
        # Ensure OPENAI_API_KEY is in environment
        self.client = OpenAI()
        self.model = model

    def embed_text(self, text: Optional[str]) -> List[float]:
        """
        Generate embedding for the given text.

        Returns a zero-vector if text is None or empty.
        """
        if not text:
            # text-embedding-3-small has 1536 dimensions
            return [0.0] * 1536

        try:
            # Replace newlines as recommended by OpenAI for better performance
            text = text.replace("\n", " ")
            response = self.client.embeddings.create(input=[text], model=self.model)
            return response.data[0].embedding
        except Exception as e:
            logger.error(f"Failed to generate embedding: {e}")
            # Fail safe or raise? For now, return zero vector to avoid breaking pipeline
            # but log error heavily.
            return [0.0] * 1536


class VectorIndexer:
    """Manages Vector Indexes in Memgraph."""

    def __init__(self, uri: str = "bolt://localhost:7687", user: str = "", password: str = ""):
        """Initialize Neo4j/Memgraph driver."""
        auth = (user, password) if user and password else None
        self.driver = GraphDatabase.driver(uri, auth=auth)
        self.embedding_service = EmbeddingService()

    def close(self):
        """Close driver."""
        self.driver.close()

    def create_indexes(self):
        """Create Vector Indexes on Table and Column nodes."""
        logger.info("Creating vector indexes...")
        with self.driver.session() as session:
            # Index on :Table(embedding)
            # using 'usearch' index provider (native in Memgraph MAGE)
            # metric: cosine, dimensions: 1536

            # Note: Memgraph syntax for vector index might differ slightly depending on version.
            # Using standard procedure approach which is explicit for MAGE usearch.

            # For :Table
            session.run(
                """
                CALL usearch.init('Table', 'embedding', 'cosine', 1536)
            """
            )

            # For :Column
            session.run(
                """
                CALL usearch.init('Column', 'embedding', 'cosine', 1536)
            """
            )

        logger.info("Vector indexes initialized.")

    def search_nodes(self, query_text: str, label: str = "Table", k: int = 5) -> List[dict]:
        """
        Search for nearest nodes using vector similarity.

        Args:
            query_text: The semantic query.
            label: 'Table' or 'Column'.
            k: Number of nearest neighbors.
        """
        vector = self.embedding_service.embed_text(query_text)

        with self.driver.session() as session:
            # usearch.search(label, property, query_vector, k)
            query = """
            CALL usearch.search($label, 'embedding', $vector, $k)
            YIELD node, score
            RETURN node, score
            """

            result = session.run(query, label=label, vector=vector, k=k)

            hits = []
            for record in result:
                node = record["node"]
                score = record["score"]
                # Convert format
                hits.append({"node": dict(node), "score": score})

            return hits


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    # Manual verification block
    indexer = VectorIndexer()
    try:
        indexer.create_indexes()
        print("✓ Indexes created.")

        # Test basic embedding and search (even if no data, should run)
        print("Testing search...")
        results = indexer.search_nodes("test query", k=1)
        print(f"✓ Search executed successfully. Hits: {len(results)}")

    finally:
        indexer.close()
