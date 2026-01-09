"""Thread-safe vector index wrapper with double buffering.

Provides safe concurrent access and hot-swap updates for vector indexes.
"""

import gc
import logging
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING, Callable, List

import numpy as np

if TYPE_CHECKING:
    from .protocol import VectorIndex

from .protocol import SearchResult

logger = logging.getLogger(__name__)


class ThreadSafeIndex:
    """Thread-safe wrapper for VectorIndex with double buffering.

    Implements the "Double Buffer" pattern for safe index updates:
    1. Shadow Index: Builds new index in background without touching live index
    2. Hot Swap: Uses lock only for pointer swap (minimal contention)
    3. Garbage Collection: Properly disposes old index after swap
    4. Error Handling: Logs errors but continues with existing index

    Usage:
        index = ThreadSafeIndex.create(dim=1536)
        results = index.search(query, k=5)

        # Background update (non-blocking)
        index.update_async(build_func)

        # Synchronous update
        index.update(build_func)
    """

    def __init__(
        self,
        initial_index: "VectorIndex",
    ) -> None:
        """Initialize thread-safe index wrapper.

        Args:
            initial_index: The active VectorIndex to wrap.
        """
        self._active_index: "VectorIndex" = initial_index
        self._swap_lock = threading.Lock()
        self._update_lock = threading.Lock()  # Prevent concurrent updates
        self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="index-update")

    @classmethod
    def create(
        cls,
        dim: int | None = None,
        **kwargs,
    ) -> "ThreadSafeIndex":
        """Create ThreadSafeIndex with HNSW backend.

        Args:
            dim: Vector dimension.
            **kwargs: Additional backend-specific arguments.

        Returns:
            ThreadSafeIndex with HNSW backend.
        """
        from .factory import create_vector_index

        primary = create_vector_index(backend="hnsw", dim=dim, **kwargs)
        return cls(primary)

    def search(self, query_vector: np.ndarray, k: int) -> List[SearchResult]:
        """Thread-safe search using active index.

        Args:
            query_vector: 1D numpy array of the query embedding.
            k: Number of neighbors to return.

        Returns:
            List of SearchResult sorted by score descending.
        """
        # Read the active index reference (atomic in Python)
        # No lock needed for reads - we just get the current pointer
        index = self._active_index
        return index.search(query_vector, k)

    def add_items(
        self,
        vectors: np.ndarray,
        ids: List[int],
        metadata: dict[int, dict] | None = None,
    ) -> None:
        """Add items to active index (NOT thread-safe for writes).

        For thread-safe updates, use update() or update_async() instead.

        Args:
            vectors: 2D numpy array of shape (n_items, dimension).
            ids: List of unique identifiers for each vector.
            metadata: Optional dict mapping id -> metadata dict.
        """
        # Direct write to active index - caller must ensure no concurrent updates
        self._active_index.add_items(vectors, ids, metadata)

    def update(self, build_func: Callable[[], "VectorIndex"]) -> bool:
        """Build new index synchronously and hot-swap.

        Args:
            build_func: Callable that returns a new VectorIndex.

        Returns:
            True if swap succeeded, False if build failed.
        """
        # Prevent concurrent updates
        with self._update_lock:
            return self._do_update(build_func)

    def update_async(self, build_func: Callable[[], "VectorIndex"]) -> None:
        """Asynchronously build new index and hot-swap.

        Non-blocking. The update runs in a background thread.

        Args:
            build_func: Callable that returns a new VectorIndex.
        """
        self._executor.submit(self._update_with_lock, build_func)

    def _update_with_lock(self, build_func: Callable[[], "VectorIndex"]) -> bool:
        """Wrap async update with lock."""
        with self._update_lock:
            return self._do_update(build_func)

    def _do_update(self, build_func: Callable[[], "VectorIndex"]) -> bool:
        """Execute index update with shadow build and pointer swap.

        1. Build new index (shadow)
        2. Acquire lock
        3. Swap pointer
        4. Release lock
        5. Dispose old index

        Args:
            build_func: Callable that returns a new VectorIndex.

        Returns:
            True if swap succeeded, False if build failed.
        """
        old_index = self._active_index
        new_index: "VectorIndex | None" = None

        try:
            # 1. Shadow Build: Build new index without touching live index
            logger.info("Building shadow index...")
            new_index = build_func()
            logger.info("Shadow index built successfully")

            # 2. Hot Swap: Acquire lock only for pointer swap
            with self._swap_lock:
                self._active_index = new_index

            logger.info("Index hot-swapped successfully")

            # 3. Garbage Collection: Dispose old index
            # Setting old_index to None allows GC to collect it
            # For hnswlib, the C++ destructor will be called
            del old_index
            gc.collect()

            return True

        except Exception as e:
            logger.error(f"Index update failed: {e}")

            # Clean up failed new index
            if new_index is not None:
                del new_index
                gc.collect()

            return False

    def load(self, path: str) -> bool:
        """Load index from disk with error handling.

        Args:
            path: Path to the saved index.

        Returns:
            True if load succeeded, False if failed.
        """

        def load_func() -> "VectorIndex":
            from .factory import create_vector_index

            new_index = create_vector_index(backend="hnsw")
            new_index.load(path)
            return new_index

        return self.update(load_func)

    def save(self, path: str) -> None:
        """Save the active index to disk.

        Args:
            path: Path to save the index.
        """
        self._active_index.save(path)

    @property
    def active_backend(self) -> str:
        """Get name of the active backend type."""
        return type(self._active_index).__name__

    def __len__(self) -> int:
        """Return number of items in active index."""
        return len(self._active_index)

    def shutdown(self) -> None:
        """Shutdown the background executor."""
        self._executor.shutdown(wait=True)

    def __del__(self) -> None:
        """Cleanup on deletion."""
        try:
            self._executor.shutdown(wait=False)
        except Exception:
            pass
