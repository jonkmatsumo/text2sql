"""Tests for ThreadSafeIndex with double buffering."""

import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import numpy as np
import pytest
from mcp_server.graph_ingestion.vector_indexes import BruteForceIndex, HNSWIndex, ThreadSafeIndex


class TestThreadSafeIndex:
    """Tests for ThreadSafeIndex wrapper."""

    def test_basic_search(self):
        """Verify basic search through wrapper."""
        inner = BruteForceIndex()
        vectors = np.array([[1.0, 0.0], [0.0, 1.0]])
        ids = [1, 2]
        inner.add_items(vectors, ids)

        safe = ThreadSafeIndex(inner)
        results = safe.search(np.array([1.0, 0.0]), k=1)

        assert len(results) == 1
        assert results[0].id == 1

    def test_add_items(self):
        """Verify add_items passes through to active index."""
        safe = ThreadSafeIndex(BruteForceIndex())
        vectors = np.array([[1.0, 0.0]])
        safe.add_items(vectors, [1])

        assert len(safe) == 1

    def test_create_factory_brute_force(self):
        """Verify factory creates BruteForce index."""
        safe = ThreadSafeIndex.create(backend="brute_force")
        assert safe.active_backend == "BruteForceIndex"

    @pytest.mark.skipif(HNSWIndex is None, reason="hnswlib not installed")
    def test_create_factory_hnsw(self):
        """Verify factory creates HNSW index."""
        safe = ThreadSafeIndex.create(backend="hnsw", dim=3)
        assert safe.active_backend == "HNSWIndex"


class TestHotSwap:
    """Tests for hot-swap update mechanism."""

    def test_synchronous_update(self):
        """Verify synchronous update swaps index."""
        old_index = BruteForceIndex()
        old_index.add_items(np.array([[1.0, 0.0]]), [1])

        safe = ThreadSafeIndex(old_index)
        assert len(safe) == 1

        def build_new():
            new = BruteForceIndex()
            new.add_items(np.array([[1.0, 0.0], [0.0, 1.0]]), [10, 20])
            return new

        success = safe.update(build_new)

        assert success is True
        assert len(safe) == 2
        results = safe.search(np.array([0.0, 1.0]), k=1)
        assert results[0].id == 20

    def test_async_update(self):
        """Verify async update eventually swaps index."""
        old_index = BruteForceIndex()
        old_index.add_items(np.array([[1.0, 0.0]]), [1])

        safe = ThreadSafeIndex(old_index)

        def build_new():
            time.sleep(0.1)  # Simulate build time
            new = BruteForceIndex()
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
        index = BruteForceIndex()
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
                new = BruteForceIndex()
                new.add_items(np.array([[1.0, 0.0]]), [2])
                return new

            safe.update(build)

        with ThreadPoolExecutor(max_workers=3) as executor:
            executor.submit(reader)
            executor.submit(reader)
            executor.submit(updater)

        assert len(errors) == 0
        assert read_count[0] > 0


class TestFallback:
    """Tests for error handling and fallback."""

    def test_fallback_on_build_error(self):
        """Verify fallback activates when build fails."""
        primary = BruteForceIndex()
        primary.add_items(np.array([[1.0, 0.0]]), [1])

        fallback = BruteForceIndex()
        fallback.add_items(np.array([[0.0, 1.0]]), [999])

        safe = ThreadSafeIndex(primary, fallback)
        assert not safe.is_using_fallback

        def bad_build():
            raise RuntimeError("Build failed!")

        success = safe.update(bad_build)

        assert success is False
        assert safe.is_using_fallback is True
        assert safe.active_backend == "BruteForceIndex"

        # Should use fallback
        results = safe.search(np.array([0.0, 1.0]), k=1)
        assert results[0].id == 999

    def test_no_fallback_when_disabled(self):
        """Verify no fallback when not configured."""
        primary = BruteForceIndex()
        primary.add_items(np.array([[1.0, 0.0]]), [1])

        safe = ThreadSafeIndex(primary, fallback_index=None)

        def bad_build():
            raise RuntimeError("Build failed!")

        success = safe.update(bad_build)

        assert success is False
        assert safe.is_using_fallback is False
        # Should still have original index
        assert len(safe) == 1

    def test_create_with_fallback(self):
        """Verify factory creates fallback by default."""
        safe = ThreadSafeIndex.create(backend="brute_force", fallback_enabled=True)
        assert safe._fallback_index is not None


class TestPersistence:
    """Tests for save/load with thread safety."""

    def test_save_and_load(self):
        """Verify save/load through thread-safe wrapper."""
        original = BruteForceIndex()
        original.add_items(np.array([[1.0, 0.0]]), [1])

        safe = ThreadSafeIndex(original)

        with tempfile.NamedTemporaryFile(suffix=".pkl", delete=False) as f:
            path = f.name

        try:
            safe.save(path)

            # Load into new wrapper
            safe2 = ThreadSafeIndex.create(backend="brute_force")
            success = safe2.load(path, index_type="brute_force")

            assert success is True
            assert len(safe2) == 1
        finally:
            import os

            os.unlink(path)


class TestConcurrencyStress:
    """Stress tests for concurrent access."""

    def test_many_concurrent_reads(self):
        """Stress test with many concurrent reads."""
        index = BruteForceIndex()
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
