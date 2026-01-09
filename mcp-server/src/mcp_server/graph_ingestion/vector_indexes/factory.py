"""Factory for creating VectorIndex instances.

Uses INDEX_BACKEND environment variable to select the backend.
"""

import os
from typing import TYPE_CHECKING

from .brute_force import BruteForceIndex

if TYPE_CHECKING:
    from .protocol import VectorIndex


def create_vector_index(
    backend: str | None = None,
    dim: int | None = None,
    max_elements: int = 100_000,
    **kwargs,
) -> "VectorIndex":
    """Create a VectorIndex based on backend selection.

    Args:
        backend: Backend type. If None, reads from INDEX_BACKEND env var.
                 Supported values: "brute_force", "hnsw".
        dim: Vector dimension (required for HNSW if not loading from disk).
        max_elements: Maximum number of elements (HNSW only).
        **kwargs: Additional backend-specific arguments.

    Returns:
        A VectorIndex implementation.

    Raises:
        ValueError: If backend is unknown.
        ImportError: If hnswlib is not installed for HNSW backend.
    """
    if backend is None:
        backend = os.getenv("INDEX_BACKEND", "hnsw")

    backend = backend.lower().strip()

    if backend == "brute_force":
        return BruteForceIndex()
    elif backend == "hnsw":
        from .hnsw import HNSWIndex

        return HNSWIndex(dim=dim, max_elements=max_elements, **kwargs)
    else:
        raise ValueError(
            f"Unknown INDEX_BACKEND: '{backend}'. " f"Supported values: 'brute_force', 'hnsw'."
        )
