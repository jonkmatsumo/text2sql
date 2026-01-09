"""Factory for creating VectorIndex instances.

Uses INDEX_BACKEND environment variable to select the backend.
"""

import os
from typing import TYPE_CHECKING

from .brute_force import BruteForceIndex

if TYPE_CHECKING:
    from .protocol import VectorIndex


def create_vector_index(backend: str | None = None) -> "VectorIndex":
    """Create a VectorIndex based on backend selection.

    Args:
        backend: Backend type. If None, reads from INDEX_BACKEND env var.
                 Supported values: "brute_force", "hnsw" (future).

    Returns:
        A VectorIndex implementation.

    Raises:
        ValueError: If backend is unknown.
        NotImplementedError: If backend is not yet implemented.
    """
    if backend is None:
        backend = os.getenv("INDEX_BACKEND", "brute_force")

    backend = backend.lower().strip()

    if backend == "brute_force":
        return BruteForceIndex()
    elif backend == "hnsw":
        raise NotImplementedError(
            "HNSW backend not yet implemented. " "Install hnswlib and implement HNSWIndex class."
        )
    else:
        raise ValueError(
            f"Unknown INDEX_BACKEND: '{backend}'. " f"Supported values: 'brute_force', 'hnsw'."
        )
