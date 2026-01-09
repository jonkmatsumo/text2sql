"""Retrieve-and-Rerank strategy for vector search.

Implements hybrid retrieval pattern to maximize Recall@K by:
1. Fetching k_candidates (10x) from ANN index
2. Retrieving full vectors for candidates
3. Computing exact similarity with vectorized numpy
4. Returning top-k by exact score
"""

import logging
import os
from typing import TYPE_CHECKING, List, Optional

import numpy as np

if TYPE_CHECKING:
    from .protocol import VectorIndex

from .protocol import SearchResult

logger = logging.getLogger(__name__)

# Expansion factor for candidate retrieval
RERANK_EXPANSION_FACTOR = 10


def search_with_rerank(
    index: "VectorIndex",
    query_vector: np.ndarray,
    k: int,
    expansion_factor: int = RERANK_EXPANSION_FACTOR,
    brute_force_index: "Optional[VectorIndex]" = None,
) -> List[SearchResult]:
    """Search with retrieve-and-rerank strategy.

    Fetches expanded candidates from ANN index, retrieves their vectors,
    computes exact similarity with vectorized numpy.dot, and returns top-k.

    Args:
        index: The VectorIndex to search (typically HNSW).
        query_vector: 1D numpy array of the query embedding.
        k: Number of final results to return.
        expansion_factor: Multiply k by this for candidate retrieval.
        brute_force_index: Optional brute-force index for recall loss validation.

    Returns:
        List of SearchResult sorted by exact score descending.
    """
    # 1. Expansion: Query for k_candidates = k * expansion_factor
    k_candidates = k * expansion_factor
    candidates = index.search(query_vector, k=k_candidates)

    if not candidates:
        return []

    # Extract candidate IDs
    candidate_ids = [c.id for c in candidates]
    candidate_metadata = {c.id: c.metadata for c in candidates}

    # 2. Vector Fetch: Retrieve full vectors for candidates
    if not hasattr(index, "get_vectors_by_ids"):
        # Fallback: index doesn't support vector retrieval
        # Just return top-k of the candidates
        logger.warning("Index doesn't support get_vectors_by_ids, skipping rerank")
        return candidates[:k]

    candidate_vectors = index.get_vectors_by_ids(candidate_ids)
    if candidate_vectors is None:
        return candidates[:k]

    # 3. Vectorized Scoring: Use numpy.dot for exact similarity
    # Normalize query (should already be normalized for IP space)
    query = query_vector.flatten().astype(np.float32)
    query_norm = np.linalg.norm(query)
    if query_norm == 0:
        return []
    query_normalized = query / query_norm

    # Vectorized dot product: (n_candidates,) = (n_candidates, dim) @ (dim,)
    # For normalized vectors, this gives cosine similarity
    exact_scores = candidate_vectors @ query_normalized

    # 4. Sort & Slice: Sort by exact score and return top-k
    sorted_indices = np.argsort(exact_scores)[::-1][:k]

    results = []
    for idx in sorted_indices:
        item_id = candidate_ids[idx]
        results.append(
            SearchResult(
                id=item_id,
                score=float(exact_scores[idx]),
                metadata=candidate_metadata.get(item_id),
            )
        )

    # 5. Validation: Log recall loss if RECORD_GOLDEN_SET is on
    if os.getenv("RECORD_GOLDEN_SET", "").lower() in ("1", "true", "on"):
        _log_recall_loss(results, brute_force_index, query_vector, k)

    return results


def _log_recall_loss(
    reranked_results: List[SearchResult],
    brute_force_index: "Optional[VectorIndex]",
    query_vector: np.ndarray,
    k: int,
) -> None:
    """Log recall loss comparing reranked results to brute-force ground truth.

    Args:
        reranked_results: Results from reranking.
        brute_force_index: Brute-force index for ground truth.
        query_vector: Original query vector.
        k: Number of results.
    """
    if brute_force_index is None:
        return

    # Get ground truth from brute-force
    ground_truth = brute_force_index.search(query_vector, k=k)
    gt_ids = set(r.id for r in ground_truth)

    # Compare with reranked results
    reranked_ids = set(r.id for r in reranked_results)

    # Recall = |intersection| / |ground_truth|
    intersection = gt_ids & reranked_ids
    recall = len(intersection) / len(gt_ids) if gt_ids else 1.0
    recall_loss = 1.0 - recall

    if recall_loss > 0:
        missed_ids = gt_ids - reranked_ids
        logger.warning(
            f"Recall Loss: {recall_loss:.2%} "
            f"(missed {len(missed_ids)} of {len(gt_ids)}: {list(missed_ids)[:5]}...)"
        )
    else:
        logger.debug(f"Perfect recall: {len(gt_ids)}/{len(gt_ids)} items matched")
