"""Vector indexes module.

Provides pluggable vector similarity search backends.
"""

from .brute_force import BruteForceIndex
from .factory import create_vector_index
from .protocol import SearchResult, VectorIndex

__all__ = [
    "VectorIndex",
    "SearchResult",
    "BruteForceIndex",
    "create_vector_index",
]
