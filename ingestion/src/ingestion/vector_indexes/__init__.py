"""Vector indexes module.

Provides pluggable vector similarity search backends.
Currently uses HNSW (hnswlib) as the sole implementation.
"""

from common.interfaces.vector_index import SearchResult, VectorIndex

from .factory import create_vector_index
from .hnsw import HNSWIndex
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
