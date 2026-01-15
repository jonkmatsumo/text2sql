from typing import Any, Dict, List, Optional, Protocol, runtime_checkable

import numpy as np

from schema.rag import FilterCriteria

from .vector_index import SearchResult


@runtime_checkable
class ExtendedVectorIndex(Protocol):
    """Extended VectorIndex protocol with structured filtering support.

    This extends the base VectorIndex with:
    - Structured filter support (FilterCriteria instead of raw strings)
    - String IDs for cross-backend compatibility (Pinecone requires client-side IDs)
    - Metadata support for richer item storage

    Note: The base VectorIndex in vector_indexes/protocol.py remains unchanged
    for backward compatibility. Use this protocol for new implementations
    that need filtering or metadata support.
    """

    def search(
        self,
        query_vector: np.ndarray,
        k: int,
        filter: Optional[FilterCriteria] = None,
    ) -> List[SearchResult]:
        """Search for k nearest neighbors with optional filtering.

        Args:
            query_vector: 1D numpy array of the query embedding.
            k: Number of neighbors to return.
            filter: Optional structured filter criteria.

        Returns:
            List of SearchResult sorted by score descending.
        """
        ...

    def add_items(
        self,
        vectors: np.ndarray,
        ids: List[str],
        metadata: Optional[List[Dict[str, Any]]] = None,
    ) -> None:
        """Add items with explicit string IDs.

        String IDs are required for Pinecone and other cloud vector DBs
        that require client-side ID generation.

        Args:
            vectors: 2D numpy array of shape (n_items, dimension).
            ids: List of unique string identifiers for each vector.
            metadata: Optional list of metadata dicts for each vector.
        """
        ...

    def delete_items(self, ids: List[str]) -> None:
        """Delete items by their IDs.

        Args:
            ids: List of item IDs to delete.
        """
        ...

    def save(self, path: str) -> None:
        """Persist the index to disk.

        Args:
            path: File path to save the index.
        """
        ...

    def load(self, path: str) -> None:
        """Load the index from disk.

        Args:
            path: File path to load the index from.
        """
        ...
