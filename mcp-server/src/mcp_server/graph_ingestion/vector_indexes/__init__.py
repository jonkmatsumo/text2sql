"""Vector indexes module.

Provides pluggable vector similarity search backends.
"""

from .brute_force import BruteForceIndex
from .factory import create_vector_index
from .protocol import SearchResult, VectorIndex
from .reranker import search_with_rerank
from .thread_safe import ThreadSafeIndex

# HNSWIndex requires hnswlib - import lazily to avoid ImportError
try:
    from .hnsw import HNSWIndex
except ImportError:
    HNSWIndex = None  # type: ignore[assignment,misc]

__all__ = [
    "VectorIndex",
    "SearchResult",
    "BruteForceIndex",
    "HNSWIndex",
    "create_vector_index",
    "search_with_rerank",
    "ThreadSafeIndex",
]
