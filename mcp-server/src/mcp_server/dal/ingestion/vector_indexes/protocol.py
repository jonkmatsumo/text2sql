"""VectorIndex Protocol and SearchResult dataclass.

Defines the interface for vector similarity search backends.
"""

from dataclasses import dataclass, field
from typing import List, Optional, Protocol

import numpy as np


@dataclass
class SearchResult:
    """Result from a vector similarity search.

    Attributes:
        id: Unique identifier of the matched item.
        score: Similarity score (higher = more similar).
        metadata: Optional dictionary of additional data.
    """

    id: int
    score: float
    metadata: Optional[dict] = field(default=None)


class VectorIndex(Protocol):
    """Protocol for vector similarity search backends.

    Implementations must provide:
    - search: Find k nearest neighbors for a query vector.
    - add_items: Add vectors with their IDs to the index.
    - save: Persist the index to disk.
    - load: Load the index from disk.
    """

    def search(self, query_vector: np.ndarray, k: int) -> List[SearchResult]:
        """Search for k nearest neighbors.

        Args:
            query_vector: 1D numpy array of the query embedding.
            k: Number of neighbors to return.

        Returns:
            List of SearchResult sorted by score descending.
        """
        ...

    def add_items(self, vectors: np.ndarray, ids: List[int]) -> None:
        """Add items to the index.

        Args:
            vectors: 2D numpy array of shape (n_items, dimension).
            ids: List of unique identifiers for each vector.
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
