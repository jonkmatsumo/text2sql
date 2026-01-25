"""HNSW vector index implementation using hnswlib.

High-performance approximate nearest neighbor search optimized for millions of vectors.
"""

from __future__ import annotations

import pickle
from pathlib import Path
from typing import TYPE_CHECKING, List, Optional

import numpy as np

if TYPE_CHECKING:
    import hnswlib  # noqa: F401


from common.interfaces.vector_index import SearchResult


class HNSWIndex:
    """HNSW approximate nearest neighbor index using hnswlib.

    Uses Inner Product (IP) space with pre-normalized vectors for maximum
    performance. This is mathematically equivalent to cosine similarity
    when vectors are L2-normalized.

    Default parameters optimized for millions of vectors:
    - M=32: Higher connectivity for better recall
    - ef_construction=200: Better graph quality during build
    - ef_search=100: Runtime tunable for speed/recall tradeoff
    """

    # Default HNSW parameters for large-scale datasets
    DEFAULT_M = 32
    DEFAULT_EF_CONSTRUCTION = 200
    DEFAULT_EF_SEARCH = 100

    def __init__(
        self,
        dim: Optional[int] = None,
        max_elements: int = 100_000,
        m: int = DEFAULT_M,
        ef_construction: int = DEFAULT_EF_CONSTRUCTION,
        ef_search: int = DEFAULT_EF_SEARCH,
    ) -> None:
        """Initialize HNSW index.

        Args:
            dim: Vector dimension. If None, will be set on first add_items().
            max_elements: Maximum number of elements the index can hold.
            m: Number of bi-directional links per node. Higher = better recall.
            ef_construction: Size of dynamic list during construction.
            ef_search: Size of dynamic list during search (can be changed later).

        Raises:
            ImportError: If hnswlib is not installed.
        """
        try:
            import hnswlib  # noqa: F811
        except ImportError:
            raise ImportError(
                "hnswlib is required for HNSWIndex. " "Install with: pip install hnswlib"
            )

        self._dim = dim
        self._max_elements = max_elements
        self._m = m
        self._ef_construction = ef_construction
        self._ef_search = ef_search

        self._index: Optional[hnswlib.Index] = None
        self._ids: List[int] = []
        self._id_to_idx: dict[int, int] = {}  # Maps external id -> internal index
        self._metadata: dict[int, dict] = {}
        # Store normalized vectors for reranking (hnswlib doesn't expose them)
        self._vectors_normalized: Optional[np.ndarray] = None

        # Initialize index if dimension is known
        if dim is not None:
            self._init_index(dim)

    def _init_index(self, dim: int) -> None:
        import hnswlib  # noqa: F811

        self._dim = dim
        self._index = hnswlib.Index(space="ip", dim=dim)
        self._index.init_index(
            max_elements=self._max_elements,
            M=self._m,
            ef_construction=self._ef_construction,
        )
        self._index.set_ef(self._ef_search)

    @staticmethod
    def _normalize(vectors: np.ndarray) -> np.ndarray:
        """L2-normalize vectors for inner product = cosine similarity.

        Args:
            vectors: 2D array of shape (n, dim)

        Returns:
            Normalized vectors with unit L2 norm.
        """
        norms = np.linalg.norm(vectors, axis=1, keepdims=True)
        # Avoid division by zero
        norms = np.where(norms == 0, 1, norms)
        return vectors / norms

    def search(self, query_vector: np.ndarray, k: int) -> List[SearchResult]:
        """Search for k nearest neighbors.

        Args:
            query_vector: 1D numpy array of the query embedding.
            k: Number of neighbors to return.

        Returns:
            List of SearchResult sorted by score descending.
        """
        if self._index is None or len(self._ids) == 0:
            return []

        # Ensure 2D and normalize
        query = query_vector.flatten().reshape(1, -1).astype(np.float32)

        # Check for zero vector
        if np.linalg.norm(query) == 0:
            return []

        query_normalized = self._normalize(query)

        # Limit k to number of elements
        k = min(k, len(self._ids))

        # hnswlib returns (labels, distances), each of shape (n_queries, k)
        labels, distances = self._index.knn_query(query_normalized, k=k)

        results = []
        for idx, dist in zip(labels[0], distances[0]):
            # Convert internal index to external id
            if idx < len(self._ids):
                item_id = self._ids[idx]

                # hnswlib IP space returns 1 - dot_product as "distance"
                # For normalized vectors: similarity = dot_product = 1 - distance
                similarity = 1.0 - float(dist)

                results.append(
                    SearchResult(
                        id=item_id,
                        score=similarity,
                        metadata=self._metadata.get(item_id),
                    )
                )

        return results

    def add_items(
        self,
        vectors: np.ndarray,
        ids: List[int],
        metadata: Optional[dict[int, dict]] = None,
    ) -> None:
        """Add items to the index with L2 normalization.

        Args:
            vectors: 2D numpy array of shape (n_items, dimension).
            ids: List of unique identifiers for each vector.
            metadata: Optional dict mapping id -> metadata dict.
        """
        if len(vectors) == 0:
            return

        # Ensure 2D and float32
        if vectors.ndim == 1:
            vectors = vectors.reshape(1, -1)
        vectors = vectors.astype(np.float32)

        if len(vectors) != len(ids):
            raise ValueError(f"vectors and ids length mismatch: {len(vectors)} vs {len(ids)}")

        # Initialize index if needed
        if self._index is None:
            self._init_index(vectors.shape[1])

        # L2 normalize before adding
        vectors_normalized = self._normalize(vectors)

        # Assign internal indices
        start_idx = len(self._ids)
        internal_ids = np.arange(start_idx, start_idx + len(ids))

        # Check capacity and resize if needed
        current_count = self._index.get_current_count()
        new_count = current_count + len(ids)
        if new_count > self._max_elements:
            self._index.resize_index(max(new_count * 2, self._max_elements * 2))
            self._max_elements = self._index.get_max_elements()

        # Add to index
        self._index.add_items(vectors_normalized, internal_ids)

        # Store normalized vectors for reranking
        if self._vectors_normalized is None:
            self._vectors_normalized = vectors_normalized.copy()
        else:
            self._vectors_normalized = np.vstack([self._vectors_normalized, vectors_normalized])

        # Track mappings
        for i, ext_id in enumerate(ids):
            self._ids.append(ext_id)
            self._id_to_idx[ext_id] = start_idx + i

        if metadata:
            self._metadata.update(metadata)

    def save(self, path: str) -> None:
        """Persist the index to disk.

        Creates two files:
        - {path}: The hnswlib binary index
        - {path}.meta: Pickle file with ids and metadata

        Args:
            path: Base file path to save the index.
        """
        if self._index is None:
            raise ValueError("Cannot save empty index")

        # Save hnswlib index
        self._index.save_index(path)

        # Save metadata separately
        meta_path = Path(path).with_suffix(".meta")
        state = {
            "dim": self._dim,
            "max_elements": self._max_elements,
            "m": self._m,
            "ef_construction": self._ef_construction,
            "ef_search": self._ef_search,
            "ids": self._ids,
            "id_to_idx": self._id_to_idx,
            "metadata": self._metadata,
            "vectors_normalized": self._vectors_normalized,
        }
        with open(meta_path, "wb") as f:
            pickle.dump(state, f)

    def load(self, path: str) -> None:
        """Load the index from disk.

        Args:
            path: Base file path to load the index from.
        """
        try:
            import hnswlib  # noqa: F811
        except ImportError:
            raise ImportError("hnswlib is required to load HNSWIndex")

        # Load metadata first to get dimension
        meta_path = Path(path).with_suffix(".meta")
        with open(meta_path, "rb") as f:
            state = pickle.load(f)

        self._dim = state["dim"]
        self._max_elements = state["max_elements"]
        self._m = state["m"]
        self._ef_construction = state["ef_construction"]
        self._ef_search = state["ef_search"]
        self._ids = state["ids"]
        self._id_to_idx = state["id_to_idx"]
        self._metadata = state["metadata"]
        self._vectors_normalized = state.get("vectors_normalized")

        # Initialize and load hnswlib index
        import hnswlib  # noqa: F811

        self._index = hnswlib.Index(space="ip", dim=self._dim)
        self._index.load_index(path, max_elements=self._max_elements)
        self._index.set_ef(self._ef_search)

    def set_ef_search(self, ef: int) -> None:
        """Set ef parameter for search (runtime tunable).

        Higher ef = better recall but slower search.

        Args:
            ef: Size of dynamic list during search.
        """
        self._ef_search = ef
        if self._index is not None:
            self._index.set_ef(ef)

    def __len__(self) -> int:
        """Return number of items in the index."""
        return len(self._ids)

    def get_vectors_by_ids(self, ids: List[int]) -> Optional[np.ndarray]:
        """Retrieve normalized vectors for given IDs.

        Args:
            ids: List of external IDs to retrieve.

        Returns:
            2D numpy array of shape (len(ids), dim), or None if unavailable.
        """
        if self._vectors_normalized is None or not ids:
            return None

        indices = []
        for ext_id in ids:
            if ext_id in self._id_to_idx:
                indices.append(self._id_to_idx[ext_id])

        if not indices:
            return None

        return self._vectors_normalized[indices].copy()

    def get_all_vectors(self) -> tuple[Optional[np.ndarray], List[int]]:
        """Retrieve all normalized vectors and their IDs.

        Returns:
            Tuple of (vectors array, list of IDs), or (None, []) if empty.
        """
        if self._vectors_normalized is None:
            return None, []
        return self._vectors_normalized.copy(), self._ids.copy()
