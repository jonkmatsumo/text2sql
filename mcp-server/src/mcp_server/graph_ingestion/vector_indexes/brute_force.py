"""Brute-force vector index implementation.

Simple O(n) cosine similarity search for small datasets or baseline comparison.
"""

import pickle
from typing import List

import numpy as np

from .protocol import SearchResult


class BruteForceIndex:
    """Brute-force cosine similarity search.

    Computes similarity against all stored vectors for each query.
    Suitable for small datasets (<10k vectors) or as a baseline.
    """

    def __init__(self) -> None:
        """Initialize empty index."""
        self._vectors: np.ndarray | None = None
        self._ids: List[int] = []
        self._metadata: dict[int, dict] = {}

    def search(self, query_vector: np.ndarray, k: int) -> List[SearchResult]:
        """Search for k nearest neighbors using cosine similarity.

        Args:
            query_vector: 1D numpy array of the query embedding.
            k: Number of neighbors to return.

        Returns:
            List of SearchResult sorted by score descending.
        """
        if self._vectors is None or len(self._ids) == 0:
            return []

        # Ensure query is 1D
        query = query_vector.flatten()

        # Normalize query
        query_norm = np.linalg.norm(query)
        if query_norm == 0:
            return []
        query_normalized = query / query_norm

        # Compute cosine similarity with all vectors
        # Vectors are stored row-wise: (n_items, dimension)
        norms = np.linalg.norm(self._vectors, axis=1)
        # Avoid division by zero
        valid_mask = norms > 0
        similarities = np.zeros(len(self._ids))
        similarities[valid_mask] = (self._vectors[valid_mask] @ query_normalized) / norms[
            valid_mask
        ]

        # Get top-k indices
        k = min(k, len(self._ids))
        top_indices = np.argpartition(similarities, -k)[-k:]
        top_indices = top_indices[np.argsort(similarities[top_indices])[::-1]]

        # Build results
        results = []
        for idx in top_indices:
            item_id = self._ids[idx]
            results.append(
                SearchResult(
                    id=item_id,
                    score=float(similarities[idx]),
                    metadata=self._metadata.get(item_id),
                )
            )

        return results

    def add_items(
        self,
        vectors: np.ndarray,
        ids: List[int],
        metadata: dict[int, dict] | None = None,
    ) -> None:
        """Add items to the index.

        Args:
            vectors: 2D numpy array of shape (n_items, dimension).
            ids: List of unique identifiers for each vector.
            metadata: Optional dict mapping id -> metadata dict.
        """
        if len(vectors) == 0:
            return

        # Ensure 2D (must happen before length check)
        if vectors.ndim == 1:
            vectors = vectors.reshape(1, -1)

        if len(vectors) != len(ids):
            raise ValueError(f"vectors and ids length mismatch: {len(vectors)} vs {len(ids)}")

        if self._vectors is None:
            self._vectors = vectors.copy()
        else:
            self._vectors = np.vstack([self._vectors, vectors])

        self._ids.extend(ids)

        if metadata:
            self._metadata.update(metadata)

    def save(self, path: str) -> None:
        """Persist the index to disk using pickle.

        Args:
            path: File path to save the index.
        """
        state = {
            "vectors": self._vectors,
            "ids": self._ids,
            "metadata": self._metadata,
        }
        with open(path, "wb") as f:
            pickle.dump(state, f)

    def load(self, path: str) -> None:
        """Load the index from disk.

        Args:
            path: File path to load the index from.
        """
        with open(path, "rb") as f:
            state = pickle.load(f)

        self._vectors = state["vectors"]
        self._ids = state["ids"]
        self._metadata = state.get("metadata", {})

    def __len__(self) -> int:
        """Return number of items in the index."""
        return len(self._ids)
