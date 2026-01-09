"""Tests for HNSWIndex implementation."""

import os
import tempfile

import numpy as np
import pytest
from mcp_server.graph_ingestion.vector_indexes import HNSWIndex, create_vector_index

# Skip all tests if hnswlib is not available
pytestmark = pytest.mark.skipif(HNSWIndex is None, reason="hnswlib not installed")


class TestHNSWIndex:
    """Tests for HNSWIndex implementation."""

    def test_empty_index_search(self):
        """Search on empty index returns empty list."""
        index = HNSWIndex(dim=3)
        query = np.array([1.0, 0.0, 0.0])
        results = index.search(query, k=5)
        assert results == []

    def test_add_and_search_single_item(self):
        """Add single item and search."""
        index = HNSWIndex(dim=3)

        vectors = np.array([[1.0, 0.0, 0.0]])
        ids = [100]
        index.add_items(vectors, ids)

        # Search with same vector - should get high similarity
        query = np.array([1.0, 0.0, 0.0])
        results = index.search(query, k=1)

        assert len(results) == 1
        assert results[0].id == 100
        # Normalized vectors with same direction should have high similarity
        assert results[0].score > 0.99

    def test_add_and_search_multiple_items(self):
        """Add multiple items and verify ranking."""
        index = HNSWIndex(dim=3)

        # Add 3 vectors (will be normalized internally)
        vectors = np.array(
            [
                [1.0, 0.0, 0.0],  # id=0
                [0.0, 1.0, 0.0],  # id=1
                [0.707, 0.707, 0.0],  # id=2, 45 degrees between 0 and 1
            ]
        )
        ids = [0, 1, 2]
        index.add_items(vectors, ids)

        # Query aligned with id=0
        query = np.array([1.0, 0.0, 0.0])
        results = index.search(query, k=3)

        assert len(results) == 3
        # Best match should be id=0
        assert results[0].id == 0
        assert results[0].score > 0.99
        # Second should be id=2 (45 degrees - ~0.707 similarity)
        assert results[1].id == 2
        assert 0.7 < results[1].score < 0.75

    def test_search_with_metadata(self):
        """Metadata should be returned in search results."""
        index = HNSWIndex(dim=3)

        vectors = np.array([[1.0, 0.0, 0.0]])
        ids = [1]
        metadata = {1: {"name": "test_table"}}
        index.add_items(vectors, ids, metadata=metadata)

        query = np.array([1.0, 0.0, 0.0])
        results = index.search(query, k=1)

        assert results[0].metadata == {"name": "test_table"}

    def test_k_larger_than_index_size(self):
        """Verify k larger than index size returns all items."""
        index = HNSWIndex(dim=2)

        vectors = np.array([[1.0, 0.0], [0.0, 1.0]])
        ids = [1, 2]
        index.add_items(vectors, ids)

        query = np.array([1.0, 0.0])
        results = index.search(query, k=100)

        assert len(results) == 2

    def test_zero_query_vector(self):
        """Zero query vector should return empty results."""
        index = HNSWIndex(dim=3)

        vectors = np.array([[1.0, 0.0, 0.0]])
        ids = [1]
        index.add_items(vectors, ids)

        query = np.array([0.0, 0.0, 0.0])
        results = index.search(query, k=1)

        assert results == []

    def test_add_items_length_mismatch(self):
        """Mismatched vectors and ids should raise ValueError."""
        index = HNSWIndex(dim=2)

        vectors = np.array([[1.0, 0.0], [0.0, 1.0]])
        ids = [1]  # Only 1 id for 2 vectors

        with pytest.raises(ValueError, match="mismatch"):
            index.add_items(vectors, ids)

    def test_add_items_incrementally(self):
        """Items can be added in multiple calls."""
        index = HNSWIndex(dim=2, max_elements=100)

        # First batch
        index.add_items(np.array([[1.0, 0.0]]), [1])
        assert len(index) == 1

        # Second batch
        index.add_items(np.array([[0.0, 1.0]]), [2])
        assert len(index) == 2

        # Search should find both
        query = np.array([0.707, 0.707])
        results = index.search(query, k=2)
        assert len(results) == 2

    def test_save_and_load(self):
        """Index can be saved and loaded."""
        index = HNSWIndex(dim=3, max_elements=1000)

        vectors = np.array([[1.0, 0.0, 0.0], [0.0, 1.0, 0.0]])
        ids = [10, 20]
        metadata = {10: {"name": "table_a"}}
        index.add_items(vectors, ids, metadata=metadata)

        with tempfile.NamedTemporaryFile(suffix=".hnsw", delete=False) as f:
            path = f.name

        meta_path = path + ".meta"  # The metadata file

        try:
            # Save
            index.save(path)

            # Load into new index
            new_index = HNSWIndex()
            new_index.load(path)

            assert len(new_index) == 2

            # Search should work
            query = np.array([1.0, 0.0, 0.0])
            results = new_index.search(query, k=1)
            assert results[0].id == 10
            assert results[0].metadata == {"name": "table_a"}
        finally:
            os.unlink(path)
            if os.path.exists(meta_path):
                os.unlink(meta_path)

    def test_1d_vector_input(self):
        """Verify 1D vector input is reshaped to 2D."""
        index = HNSWIndex(dim=3)

        # Pass 1D vector (should be reshaped)
        vector = np.array([1.0, 0.0, 0.0])
        ids = [1]
        index.add_items(vector, ids)

        assert len(index) == 1

    def test_set_ef_search(self):
        """Verify ef_search can be changed at runtime."""
        index = HNSWIndex(dim=3, ef_search=50)

        vectors = np.array([[1.0, 0.0, 0.0]])
        ids = [1]
        index.add_items(vectors, ids)

        # Change ef_search - should not raise
        index.set_ef_search(200)
        assert index._ef_search == 200

    def test_auto_resize(self):
        """Index should auto-resize when exceeding max_elements."""
        index = HNSWIndex(dim=2, max_elements=2)

        # Add more than max_elements
        for i in range(5):
            vectors = np.array([[float(i), 1.0]])
            index.add_items(vectors, [i])

        assert len(index) == 5

    def test_normalization_equivalence(self):
        """Verify IP with normalization equals cosine similarity."""
        index = HNSWIndex(dim=3)

        # Non-unit vectors
        vectors = np.array(
            [
                [3.0, 0.0, 0.0],  # Should be normalized to [1, 0, 0]
                [0.0, 5.0, 0.0],  # Should be normalized to [0, 1, 0]
            ]
        )
        ids = [0, 1]
        index.add_items(vectors, ids)

        # Query with non-unit vector (will be normalized)
        query = np.array([10.0, 0.0, 0.0])
        results = index.search(query, k=1)

        # Should match id=0 with high similarity despite different magnitudes
        assert results[0].id == 0
        assert results[0].score > 0.99

    def test_deferred_init(self):
        """Index can be initialized without dimension, set on first add."""
        index = HNSWIndex()  # No dim specified
        assert index._index is None

        # First add_items sets dimension
        vectors = np.array([[1.0, 0.0, 0.0]])
        ids = [1]
        index.add_items(vectors, ids)

        assert index._dim == 3
        assert index._index is not None
        assert len(index) == 1


class TestHNSWFactory:
    """Tests for factory with HNSW backend."""

    def test_create_hnsw_index(self):
        """Factory should create HNSWIndex for 'hnsw' backend."""
        index = create_vector_index(backend="hnsw", dim=128)
        assert isinstance(index, HNSWIndex)

    def test_create_hnsw_with_params(self):
        """Factory should pass parameters to HNSWIndex."""
        index = create_vector_index(
            backend="hnsw",
            dim=384,
            max_elements=500_000,
            m=48,
            ef_construction=300,
        )
        assert isinstance(index, HNSWIndex)
        assert index._dim == 384
        assert index._m == 48
        assert index._ef_construction == 300

    def test_env_var_hnsw(self):
        """INDEX_BACKEND=hnsw should create HNSWIndex."""
        with pytest.MonkeyPatch.context() as mp:
            mp.setenv("INDEX_BACKEND", "hnsw")
            index = create_vector_index(dim=64)
            assert isinstance(index, HNSWIndex)


class TestHNSWProtocolCompliance:
    """Verify HNSWIndex satisfies VectorIndex protocol."""

    def test_hnsw_has_required_methods(self):
        """Verify HNSWIndex has all Protocol methods."""
        index = HNSWIndex(dim=3)

        assert callable(getattr(index, "search", None))
        assert callable(getattr(index, "add_items", None))
        assert callable(getattr(index, "save", None))
        assert callable(getattr(index, "load", None))

    def test_structural_subtyping(self):
        """Verify HNSWIndex works as VectorIndex."""
        from mcp_server.graph_ingestion.vector_indexes import VectorIndex

        index: VectorIndex = HNSWIndex(dim=3)

        vectors = np.array([[1.0, 0.0, 0.0]])
        index.add_items(vectors, [1])
        results = index.search(np.array([1.0, 0.0, 0.0]), k=1)
        assert len(results) == 1
