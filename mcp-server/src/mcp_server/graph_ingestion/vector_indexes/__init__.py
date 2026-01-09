"""Vector indexes module.

Provides pluggable vector similarity search backends.
Currently uses HNSW (hnswlib) as the sole implementation.
"""

from .factory import create_vector_index
from .hnsw import HNSWIndex
from .protocol import SearchResult, VectorIndex
from .reranker import search_with_rerank
from .thread_safe import ThreadSafeIndex

__all__ = [
    "VectorIndex",
    "SearchResult",
    "HNSWIndex",
    "create_vector_index",
    "search_with_rerank",
    "ThreadSafeIndex",
]
