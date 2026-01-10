"""Vector indexing and search for Memgraph schema graph.

Includes adaptive thresholding to filter low-quality vector matches.
"""

import logging
import math
from typing import List, Optional

from mcp_server.dal.memgraph import MemgraphStore
from openai import OpenAI

logger = logging.getLogger(__name__)

# Adaptive thresholding constants
# Relaxed thresholds to ensure dimension tables (e.g., language) are included
MIN_SCORE_ABSOLUTE = 0.45  # Lowered from 0.55 to catch indirect semantic matches
SCORE_DROP_TOLERANCE = 0.15  # Increased from 0.08 to allow more score variance


class EmbeddingService:
    """Service to generate vector embeddings using OpenAI."""

    def __init__(self, model: str = "text-embedding-3-small"):
        """Initialize OpenAI client."""
        self.client = OpenAI()
        self.model = model

    def embed_text(self, text: Optional[str]) -> List[float]:
        """Generate embedding for the given text.

        Returns a zero-vector if text is None or empty.
        """
        if not text:
            return [0.0] * 1536

        try:
            text = text.replace("\n", " ")
            response = self.client.embeddings.create(input=[text], model=self.model)
            return response.data[0].embedding
        except Exception as e:
            logger.error(f"Failed to generate embedding: {e}")
            return [0.0] * 1536


def cosine_similarity(vec1: List[float], vec2: List[float]) -> float:
    """Compute cosine similarity between two vectors."""
    if not vec1 or not vec2 or len(vec1) != len(vec2):
        return 0.0

    dot_product = sum(a * b for a, b in zip(vec1, vec2))
    norm1 = math.sqrt(sum(a * a for a in vec1))
    norm2 = math.sqrt(sum(b * b for b in vec2))

    if norm1 == 0 or norm2 == 0:
        return 0.0

    return dot_product / (norm1 * norm2)


def apply_adaptive_threshold(hits: List[dict]) -> List[dict]:
    """Apply adaptive thresholding to filter low-quality matches.

    Algorithm:
    1. Calculate best_score from top hit
    2. Compute threshold = max(MIN_SCORE_ABSOLUTE, best_score - SCORE_DROP_TOLERANCE)
    3. Keep only hits above threshold
    4. Fallback: if empty, return top 3 hits (to ensure broader context)

    Args:
        hits: List of dicts with 'score' key, sorted by score descending

    Returns:
        Filtered list of hits
    """
    if not hits:
        return []

    best_score = hits[0]["score"]
    threshold = max(MIN_SCORE_ABSOLUTE, best_score - SCORE_DROP_TOLERANCE)

    filtered = [h for h in hits if h["score"] >= threshold]

    # Fallback: return top 3 when all below threshold (ensures broader context)
    if not filtered and hits:
        fallback_count = min(3, len(hits))
        logger.warning(
            f"All hits below threshold {threshold:.3f}, returning top-{fallback_count} fallback "
            f"(best_score={best_score:.3f})"
        )
        return hits[:fallback_count]

    if len(filtered) < len(hits):
        logger.info(
            f"Adaptive threshold {threshold:.3f} filtered {len(hits)} -> {len(filtered)} hits"
        )

    return filtered


class VectorIndexer:
    """Manages vector search in Memgraph.

    Uses brute-force cosine similarity (usearch not available in base Memgraph).
    Includes adaptive thresholding to filter low-quality matches.
    """

    def __init__(
        self,
        uri: str = "bolt://localhost:7687",
        user: str = "",
        password: str = "",
        store: Optional[MemgraphStore] = None,
    ):
        """Initialize Memgraph store.

        Args:
            uri: Bolt URI for Memgraph connection.
            user: Username for authentication.
            password: Password for authentication.
            store: Optional existing MemgraphStore instance.
        """
        if store:
            self.store = store
            self.owns_store = False
        else:
            self.store = MemgraphStore(uri, user, password)
            self.owns_store = True

        self.embedding_service = EmbeddingService()

    def close(self):
        """Close store if owned."""
        if self.owns_store:
            self.store.close()

    @property
    def driver(self):
        """Access underlying driver for legacy support."""
        return self.store.driver

    def create_indexes(self):
        """Create property indexes to speed up node retrieval.

        Note: Vector indexes (usearch) are not available in base Memgraph.
        We use property indexes for filtering, then compute similarity in Python.
        """
        logger.info("Creating property indexes...")
        with self.driver.session() as session:
            try:
                session.run("CREATE INDEX ON :Table(name)")
            except Exception:
                pass  # Index may already exist
            try:
                session.run("CREATE INDEX ON :Column(name, table)")
            except Exception:
                pass  # Index may already exist

        logger.info("Property indexes created.")

    def search_nodes(
        self,
        query_text: str,
        label: str = "Table",
        k: int = 5,
        apply_threshold: bool = True,
    ) -> List[dict]:
        """Search for nearest nodes using brute-force cosine similarity.

        Includes adaptive thresholding to filter low-quality matches.

        Args:
            query_text: The semantic query.
            label: 'Table' or 'Column'.
            k: Number of nearest neighbors to return (before thresholding).
            apply_threshold: Whether to apply adaptive thresholding.

        Returns:
            List of dicts with 'node' and 'score' keys, sorted by score desc.
        """
        query_vector = self.embedding_service.embed_text(query_text)

        with self.driver.session() as session:
            # Fetch all nodes of the given label that have embeddings
            query = f"""
            MATCH (n:{label})
            WHERE n.embedding IS NOT NULL
            RETURN n, n.embedding as embedding
            """

            result = session.run(query)

            hits = []
            for record in result:
                node = record["n"]
                node_embedding = record["embedding"]

                if not node_embedding:
                    continue

                score = cosine_similarity(query_vector, node_embedding)

                # Convert node to dict, excluding embedding
                node_dict = dict(node)
                if "embedding" in node_dict:
                    del node_dict["embedding"]

                hits.append({"node": node_dict, "score": score})

            # Sort by score descending
            hits.sort(key=lambda x: x["score"], reverse=True)

            # Take top k first
            hits = hits[:k]

            # Apply adaptive thresholding
            if apply_threshold:
                hits = apply_adaptive_threshold(hits)

            return hits


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    indexer = VectorIndexer()
    try:
        indexer.create_indexes()
        print("✓ Indexes created.")

        print("Testing search...")
        results = indexer.search_nodes("test query", k=5)
        print(f"✓ Search executed successfully. Hits: {len(results)}")
        for r in results:
            print(f"  - {r['node'].get('name', 'N/A')}: {r['score']:.3f}")

    finally:
        indexer.close()
