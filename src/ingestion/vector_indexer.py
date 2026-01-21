"""Vector indexing and search for Memgraph schema graph.

Includes adaptive thresholding to filter low-quality vector matches.
"""

import asyncio
import logging
import time
from typing import List, Optional

from openai import AsyncOpenAI

from common.interfaces import GraphStore
from common.telemetry import Telemetry

logger = logging.getLogger(__name__)

# Adaptive thresholding constants
# Relaxed thresholds to ensure dimension tables (e.g., language) are included
MIN_SCORE_ABSOLUTE = 0.45  # Lowered from 0.55 to catch indirect semantic matches
SCORE_DROP_TOLERANCE = 0.15  # Increased from 0.08 to allow more score variance


class EmbeddingService:
    """Service to generate vector embeddings using OpenAI."""

    def __init__(self, model: str = "text-embedding-3-small"):
        """Initialize OpenAI client."""
        self.client = AsyncOpenAI()
        self.model = model

    async def embed_text(self, text: Optional[str]) -> List[float]:
        """Generate embedding for the given text.

        Returns a zero-vector if text is None or empty.
        """
        if not text:
            return [0.0] * 1536

        try:
            text = text.replace("\n", " ")
            response = await self.client.embeddings.create(input=[text], model=self.model)
            return response.data[0].embedding
        except Exception as e:
            logger.error(f"Failed to generate embedding: {e}")
            return [0.0] * 1536


def apply_adaptive_threshold(hits: List[dict]) -> tuple[List[dict], float]:
    """Apply adaptive thresholding to filter low-quality matches.

    Algorithm:
    1. Calculate best_score from top hit
    2. Compute threshold = max(MIN_SCORE_ABSOLUTE, best_score - SCORE_DROP_TOLERANCE)
    3. Keep only hits above threshold
    4. Fallback: if empty, return top 3 hits (to ensure broader context)

    Args:
        hits: List of dicts with 'score' key, sorted by score descending

    Returns:
        Tuple of (Filtered list of hits, threshold used)
    """
    if not hits:
        return [], 0.0

    best_score = hits[0]["score"]
    threshold = max(MIN_SCORE_ABSOLUTE, best_score - SCORE_DROP_TOLERANCE)

    filtered = [h for h in hits if h["score"] >= threshold]

    # Fallback: return top 3 when all below threshold (ensures broader context)
    if not filtered and hits:
        fallback_count = min(3, len(hits))
        logger.debug(
            f"All hits below threshold {threshold:.3f}, returning top-{fallback_count} fallback "
            f"(best_score={best_score:.3f})"
        )
        return hits[:fallback_count], threshold

    if len(filtered) < len(hits):
        logger.debug(
            f"Adaptive threshold {threshold:.3f} filtered {len(hits)} -> {len(filtered)} hits"
        )

    return filtered, threshold


class VectorIndexer:
    """Manages vector search in Memgraph.

    Uses brute-force cosine similarity (usearch not available in base Memgraph).
    Includes adaptive thresholding to filter low-quality matches.
    """

    def __init__(
        self,
        store: GraphStore,
    ):
        """Initialize with GraphStore.

        Args:
            store: GraphStore instance.
        """
        if store is None:
            raise ValueError("store is required")

        self.store = store
        self.owns_store = False

        self.embedding_service = EmbeddingService()

    def close(self):
        """Close store if owned."""
        if self.owns_store:
            self.store.close()

    @property
    def driver(self):
        """Access underlying driver for legacy support."""
        return self.store.driver

    async def search_nodes(
        self,
        query_text: str,
        label: str = "Table",
        k: int = 5,
        apply_threshold: bool = True,
    ) -> List[dict]:
        """Search for nearest nodes using Memgraph HNSW ANN or vector scan.

        Delegates to DAL for query execution.
        Includes adaptive thresholding to filter low-quality matches.

        Contract behavior:
        - Return shape: List[{"node": dict, "score": float}]
        - 'node': Dictionary of node properties (excluding 'embedding')
        - 'score': Cosine similarity (0.0 to 1.0), NOT distance. Higher is better.
        - Sorting: Descending score (best match first).
        - Labels: Supports 'Table' or 'Column'.
        - Missing embeddings: Nodes with NULL embeddings are silently skipped.
        - Adaptive thresholding: Applied after top-k selection if enabled.

        Args:
            query_text: The semantic query.
            label: 'Table' or 'Column'.
            k: Number of nearest neighbors to return (before thresholding).
            apply_threshold: Whether to apply adaptive thresholding.

        Returns:
            List of dicts with 'node' and 'score' keys, sorted by score desc.
        """
        query_vector = await self.embedding_service.embed_text(query_text)

        def _run_search():
            start_time = time.monotonic()

            span_attributes = {
                "db.system": "memgraph",
                "db.operation": "ANN_SEARCH",
                "vector.label": label,
                "vector.top_k": k,
            }

            with Telemetry.start_span(
                "vector_seed_selection.ann", attributes=span_attributes
            ) as span:
                try:
                    if not query_vector:
                        return []

                    # Delegate to DAL
                    mapped_hits = self.store.search_ann_seeds(label, query_vector, k)

                    # Apply adaptive thresholding
                    threshold_val = 0.0
                    if apply_threshold and mapped_hits:
                        mapped_hits, threshold_val = apply_adaptive_threshold(mapped_hits)

                    elapsed_ms = (time.monotonic() - start_time) * 1000

                    log_payload = {
                        "event": "memgraph_ann_seed_search",
                        "label": label,
                        "top_k": k,
                        "returned_count": len(mapped_hits),
                        "elapsed_ms": elapsed_ms,
                        "threshold_applied": apply_threshold,
                    }
                    if apply_threshold:
                        log_payload["threshold_value"] = threshold_val

                    # Add dynamic attributes to span
                    span.set_attribute("vector.returned_count", len(mapped_hits))
                    span.set_attribute("vector.threshold_applied", apply_threshold)
                    if apply_threshold:
                        span.set_attribute("vector.threshold_value", threshold_val)

                    logger.info(
                        f"ANN search completed for label={label}, returned {len(mapped_hits)} hits",
                        extra=log_payload,
                    )

                    Telemetry.set_span_status(span, success=True)
                    return mapped_hits
                except Exception as e:
                    elapsed_ms = (time.monotonic() - start_time) * 1000
                    logger.error(
                        "ANN search failed",
                        exc_info=True,
                        extra={
                            "event": "memgraph_ann_seed_search_failed",
                            "error_type": type(e).__name__,
                            "label": label,
                            "top_k": k,
                            "elapsed_ms": elapsed_ms,
                        },
                    )
                    Telemetry.set_span_status(span, success=False, error=e)
                    # Re-raise to preserve contract; failure handled upstream.
                    raise

        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, _run_search)
