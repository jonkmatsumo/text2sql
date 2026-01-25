"""Tests for ThreadSafeIndex with double buffering."""

import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch

import numpy as np

from common.interfaces.vector_index import SearchResult
from ingestion.vector_indexes import ThreadSafeIndex


class MockHNSWIndex:
    """Mock HNSW index for testing without native binaries."""

    def __init__(
        self, dim: int = None, max_elements=100000, m=16, ef_construction=200, ef_search=10
    ):
        """Initialize mock index."""
        self.dim = dim
        self._dim = dim
        self._m = m
        self._ef_construction = ef_construction
        self._ef_search = ef_search
        self._index = "mock_index" if dim is not None else None
        self.items = []
        self.ids = []
        self.metadata = {}

    def add_items(self, vectors, ids, metadata=None):
        """Add items to the index."""
        # Handle 1D input first
        if getattr(vectors, "ndim", 0) == 1:
            vectors = vectors.reshape(1, -1)

        # Ensure vectors are at least 2D
        if len(vectors.shape) == 1:
            vectors = vectors.reshape(1, -1)

        if len(vectors) != len(ids):
            raise ValueError(f"vectors and ids length mismatch: {len(vectors)} vs {len(ids)}")

        # Determine dim on first add if not set
        if self.dim is None:
            self.dim = vectors.shape[1]
            self._dim = vectors.shape[1]
            self._index = "mock_index"

        if len(self.items) == 0:
            self.items = vectors
        else:
            self.items = np.vstack([self.items, vectors])

        self.ids.extend(ids)
        if metadata:
            self.metadata.update(metadata)

    def search(self, query_vector, k):
        """Perform simple dot product search."""
        if len(self.items) == 0:
            return []

        # Normalize query
        norm = np.linalg.norm(query_vector)
        if norm == 0:
            return []

        query = query_vector / norm

        # Simple linear scan
        scores = []
        for i, item in enumerate(self.items):
            # Normalize item
            item_norm = np.linalg.norm(item)
            if item_norm > 0:
                vec = item / item_norm
            else:
                vec = item

            score = np.dot(query, vec)
            scores.append((score, self.ids[i]))

        # Sort desc
        scores.sort(key=lambda x: x[0], reverse=True)

        results = []
        for score, id in scores[:k]:
            results.append(SearchResult(id=id, score=float(score), metadata=self.metadata.get(id)))

        return results

    def save(self, path):
        """Mock save using pickle."""
        import pickle

        with open(path, "wb") as f:
            pickle.dump(
                {"dim": self.dim, "items": self.items, "ids": self.ids, "metadata": self.metadata},
                f,
            )

    def load(self, path):
        """Mock load using pickle."""
        import pickle

        with open(path, "rb") as f:
            state = pickle.load(f)
        self.dim = state["dim"]
        self._dim = state["dim"]
        self.items = state["items"]
        self.ids = state["ids"]
        self.metadata = state["metadata"]
        self._index = "mock_index"

    def __len__(self):
        """Return size of index."""
        return len(self.ids)


class TestThreadSafeIndex:
    """Tests for ThreadSafeIndex wrapper."""

    def test_basic_search(self):
        """Verify basic search through wrapper."""
        inner = MockHNSWIndex(dim=2)
        vectors = np.array([[1.0, 0.0], [0.0, 1.0]])
        ids = [1, 2]
        inner.add_items(vectors, ids)

        safe = ThreadSafeIndex(inner)
        results = safe.search(np.array([1.0, 0.0]), k=1)

        assert len(results) == 1
        assert results[0].id == 1

    def test_add_items(self):
        """Verify add_items passes through to active index."""
        safe = ThreadSafeIndex(MockHNSWIndex(dim=2))
        vectors = np.array([[1.0, 0.0]])
        safe.add_items(vectors, [1])

        assert len(safe) == 1

    def test_create_factory(self):
        """Verify factory creates HNSW index."""
        with patch("ingestion.vector_indexes.factory.create_vector_index") as mock_create:
            mock_create.return_value = MockHNSWIndex(dim=3)

            safe = ThreadSafeIndex.create(dim=3)
            # ThreadSafeIndex checks type name for active_backend
            assert safe.active_backend == "MockHNSWIndex"


class TestHotSwap:
    """Tests for hot-swap update mechanism."""

    def test_synchronous_update(self):
        """Verify synchronous update swaps index."""
        old_index = MockHNSWIndex(dim=2)
        old_index.add_items(np.array([[1.0, 0.0]]), [1])

        safe = ThreadSafeIndex(old_index)
        assert len(safe) == 1

        def build_new():
            new = MockHNSWIndex(dim=2)
            new.add_items(np.array([[1.0, 0.0], [0.0, 1.0]]), [10, 20])
            return new

        success = safe.update(build_new)

        assert success is True
        assert len(safe) == 2
        results = safe.search(np.array([0.0, 1.0]), k=1)
        assert results[0].id == 20

    def test_async_update(self):
        """Verify async update eventually swaps index."""
        old_index = MockHNSWIndex(dim=2)
        old_index.add_items(np.array([[1.0, 0.0]]), [1])

        safe = ThreadSafeIndex(old_index)

        def build_new():
            time.sleep(0.1)  # Simulate build time
            new = MockHNSWIndex(dim=2)
            new.add_items(np.array([[1.0, 0.0], [0.0, 1.0]]), [10, 20])
            return new

        safe.update_async(build_new)

        # Initially still has old index
        time.sleep(0.05)

        # Wait for update to complete
        time.sleep(0.2)
        safe.shutdown()

        assert len(safe) == 2

    def test_concurrent_reads_during_swap(self):
        """Verify reads continue during swap without errors."""
        index = MockHNSWIndex(dim=2)
        index.add_items(np.array([[1.0, 0.0]]), [1])
        safe = ThreadSafeIndex(index)

        errors = []
        read_count = [0]

        def reader():
            for _ in range(20):
                try:
                    results = safe.search(np.array([1.0, 0.0]), k=1)
                    assert len(results) >= 0  # May be 0 during swap
                    read_count[0] += 1
                except Exception as e:
                    errors.append(e)
                time.sleep(0.01)

        def updater():
            time.sleep(0.05)  # Let reads start

            def build():
                new = MockHNSWIndex(dim=2)
                new.add_items(np.array([[1.0, 0.0]]), [2])
                return new

            safe.update(build)

        with ThreadPoolExecutor(max_workers=3) as executor:
            executor.submit(reader)
            executor.submit(reader)
            executor.submit(updater)

        assert len(errors) == 0
        assert read_count[0] > 0


class TestErrorHandling:
    """Tests for error handling."""

    def test_continues_with_original_on_build_error(self):
        """Verify original index kept when build fails."""
        primary = MockHNSWIndex(dim=2)
        primary.add_items(np.array([[1.0, 0.0]]), [1])

        safe = ThreadSafeIndex(primary)

        def bad_build():
            raise RuntimeError("Build failed!")

        success = safe.update(bad_build)

        assert success is False
        # Should still have original index
        assert len(safe) == 1
        results = safe.search(np.array([1.0, 0.0]), k=1)
        assert results[0].id == 1


class TestPersistence:
    """Tests for save/load with thread safety."""

    def test_save_and_load(self):
        """Verify save/load through thread-safe wrapper."""
        original = MockHNSWIndex(dim=2)
        original.add_items(np.array([[1.0, 0.0]]), [1])

        safe = ThreadSafeIndex(original)

        with tempfile.NamedTemporaryFile(suffix=".hnsw", delete=False) as f:
            path = f.name

        try:
            safe.save(path)

            # Load into new wrapper
            # Use mock factory
            with patch(
                "ingestion.vector_indexes.factory.create_vector_index",
                return_value=MockHNSWIndex(dim=2),
            ):
                safe2 = ThreadSafeIndex.create(dim=2)
                success = safe2.load(path)

            assert success is True
            assert len(safe2) == 1
        finally:
            import os

            os.unlink(path)
            meta_path = path + ".meta"
            if os.path.exists(meta_path):
                os.unlink(meta_path)


class TestConcurrencyStress:
    """Stress tests for concurrent access."""

    def test_many_concurrent_reads(self):
        """Stress test with many concurrent reads."""
        index = MockHNSWIndex(dim=10)
        vectors = np.random.rand(100, 10).astype(np.float32)
        ids = list(range(100))
        index.add_items(vectors, ids)

        safe = ThreadSafeIndex(index)
        errors = []
        success_count = [0]
        lock = threading.Lock()

        def reader(thread_id):
            for _ in range(50):
                try:
                    query = np.random.rand(10).astype(np.float32)
                    results = safe.search(query, k=5)
                    assert len(results) == 5
                    with lock:
                        success_count[0] += 1
                except Exception as e:
                    errors.append((thread_id, e))

        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(reader, i) for i in range(10)]
            for f in futures:
                f.result()

        assert len(errors) == 0
        assert success_count[0] == 500  # 10 threads × 50 reads
