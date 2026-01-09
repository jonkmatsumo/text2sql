"""Tests for vector_indexes module."""

import os
import tempfile
from unittest.mock import patch

import numpy as np
import pytest
from mcp_server.graph_ingestion.vector_indexes import (
    BruteForceIndex,
    SearchResult,
    VectorIndex,
    create_vector_index,
)


class TestSearchResult:
    """Tests for SearchResult dataclass."""

    def test_basic_creation(self):
        """Test basic SearchResult creation."""
        result = SearchResult(id=1, score=0.95)
        assert result.id == 1
        assert result.score == 0.95
        assert result.metadata is None

    def test_with_metadata(self):
        """Test SearchResult with metadata."""
        meta = {"name": "test_table", "description": "A test table"}
        result = SearchResult(id=42, score=0.8, metadata=meta)
        assert result.metadata == meta
        assert result.metadata["name"] == "test_table"


class TestBruteForceIndex:
    """Tests for BruteForceIndex implementation."""

    def test_empty_index_search(self):
        """Search on empty index returns empty list."""
        index = BruteForceIndex()
        query = np.array([1.0, 0.0, 0.0])
        results = index.search(query, k=5)
        assert results == []

    def test_add_and_search_single_item(self):
        """Add single item and search."""
        index = BruteForceIndex()

        # Add a single vector
        vectors = np.array([[1.0, 0.0, 0.0]])
        ids = [100]
        index.add_items(vectors, ids)

        # Search with same vector - should get perfect match
        query = np.array([1.0, 0.0, 0.0])
        results = index.search(query, k=1)

        assert len(results) == 1
        assert results[0].id == 100
        assert results[0].score == pytest.approx(1.0, rel=1e-5)

    def test_add_and_search_multiple_items(self):
        """Add multiple items and verify ranking."""
        index = BruteForceIndex()

        # Add 3 vectors
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
        assert results[0].score == pytest.approx(1.0, rel=1e-5)
        # Second should be id=2 (45 degrees)
        assert results[1].id == 2
        assert results[1].score == pytest.approx(0.707, rel=1e-2)
        # Third should be id=1 (orthogonal)
        assert results[2].id == 1
        assert results[2].score == pytest.approx(0.0, abs=1e-5)

    def test_search_with_metadata(self):
        """Metadata should be returned in search results."""
        index = BruteForceIndex()

        vectors = np.array([[1.0, 0.0, 0.0]])
        ids = [1]
        metadata = {1: {"name": "test_table"}}
        index.add_items(vectors, ids, metadata=metadata)

        query = np.array([1.0, 0.0, 0.0])
        results = index.search(query, k=1)

        assert results[0].metadata == {"name": "test_table"}

    def test_k_larger_than_index_size(self):
        """Verify k larger than index size returns all items."""
        index = BruteForceIndex()

        vectors = np.array([[1.0, 0.0], [0.0, 1.0]])
        ids = [1, 2]
        index.add_items(vectors, ids)

        query = np.array([1.0, 0.0])
        results = index.search(query, k=100)

        assert len(results) == 2

    def test_zero_query_vector(self):
        """Zero query vector should return empty results."""
        index = BruteForceIndex()

        vectors = np.array([[1.0, 0.0, 0.0]])
        ids = [1]
        index.add_items(vectors, ids)

        query = np.array([0.0, 0.0, 0.0])
        results = index.search(query, k=1)

        assert results == []

    def test_add_items_length_mismatch(self):
        """Mismatched vectors and ids should raise ValueError."""
        index = BruteForceIndex()

        vectors = np.array([[1.0, 0.0], [0.0, 1.0]])
        ids = [1]  # Only 1 id for 2 vectors

        with pytest.raises(ValueError, match="mismatch"):
            index.add_items(vectors, ids)

    def test_add_items_incrementally(self):
        """Items can be added in multiple calls."""
        index = BruteForceIndex()

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
        index = BruteForceIndex()

        vectors = np.array([[1.0, 0.0, 0.0], [0.0, 1.0, 0.0]])
        ids = [10, 20]
        metadata = {10: {"name": "table_a"}}
        index.add_items(vectors, ids, metadata=metadata)

        with tempfile.NamedTemporaryFile(suffix=".pkl", delete=False) as f:
            path = f.name

        try:
            # Save
            index.save(path)

            # Load into new index
            new_index = BruteForceIndex()
            new_index.load(path)

            assert len(new_index) == 2

            # Search should work
            query = np.array([1.0, 0.0, 0.0])
            results = new_index.search(query, k=1)
            assert results[0].id == 10
            assert results[0].metadata == {"name": "table_a"}
        finally:
            os.unlink(path)

    def test_1d_vector_input(self):
        """1D vector input should be reshaped to 2D."""
        index = BruteForceIndex()

        # Pass 1D vector (should be reshaped)
        vector = np.array([1.0, 0.0, 0.0])
        ids = [1]
        index.add_items(vector, ids)

        assert len(index) == 1


class TestFactory:
    """Tests for create_vector_index factory."""

    def test_default_is_brute_force(self):
        """Default backend should be BruteForceIndex."""
        with patch.dict(os.environ, {}, clear=True):
            # Remove INDEX_BACKEND if set
            os.environ.pop("INDEX_BACKEND", None)
            index = create_vector_index()
            assert isinstance(index, BruteForceIndex)

    def test_explicit_brute_force(self):
        """Explicit brute_force should work."""
        index = create_vector_index(backend="brute_force")
        assert isinstance(index, BruteForceIndex)

    def test_case_insensitive(self):
        """Backend name should be case-insensitive."""
        index = create_vector_index(backend="BRUTE_FORCE")
        assert isinstance(index, BruteForceIndex)

    def test_env_var_selection(self):
        """INDEX_BACKEND env var should select backend."""
        with patch.dict(os.environ, {"INDEX_BACKEND": "brute_force"}):
            index = create_vector_index()
            assert isinstance(index, BruteForceIndex)

    def test_hnsw_backend_available(self):
        """HNSW backend should work when hnswlib is installed."""
        from mcp_server.graph_ingestion.vector_indexes import HNSWIndex

        if HNSWIndex is None:
            pytest.skip("hnswlib not installed")

        index = create_vector_index(backend="hnsw", dim=64)
        assert isinstance(index, HNSWIndex)

    def test_unknown_backend_error(self):
        """Unknown backend should raise ValueError."""
        with pytest.raises(ValueError, match="Unknown INDEX_BACKEND"):
            create_vector_index(backend="unknown_backend")


class TestProtocolCompliance:
    """Verify BruteForceIndex satisfies VectorIndex protocol."""

    def test_brute_force_has_required_methods(self):
        """Verify BruteForceIndex has all Protocol methods."""
        index = BruteForceIndex()

        # Check method existence and callability
        assert callable(getattr(index, "search", None))
        assert callable(getattr(index, "add_items", None))
        assert callable(getattr(index, "save", None))
        assert callable(getattr(index, "load", None))

    def test_structural_subtyping(self):
        """Verify BruteForceIndex satisfies VectorIndex structurally."""
        # Python's Protocol uses structural subtyping at runtime with isinstance
        # when using @runtime_checkable. Without it, we just verify duck typing works.
        index: VectorIndex = BruteForceIndex()

        # This should work without type errors
        vectors = np.array([[1.0, 0.0]])
        index.add_items(vectors, [1])
        results = index.search(np.array([1.0, 0.0]), k=1)
        assert len(results) == 1
