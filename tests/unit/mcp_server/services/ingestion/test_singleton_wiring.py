from pathlib import Path
from unittest.mock import MagicMock, patch

from common.interfaces import GraphStore
from dal.factory import get_graph_store, reset_singletons
from dal.memgraph import MemgraphStore
from ingestion.vector_indexer import VectorIndexer


def test_dal_factory_is_singleton():
    """Verify that get_graph_store returns the same instance."""
    reset_singletons()
    store1 = get_graph_store()
    store2 = get_graph_store()
    assert store1 is store2
    assert isinstance(store1, GraphStore)
    assert isinstance(store1, MemgraphStore)


def test_vector_indexer_uses_injected_store():
    """Confirm VectorIndexer uses the provided store instance."""
    mock_store = MagicMock(spec=GraphStore)
    with patch("ingestion.vector_indexer.AsyncOpenAI"):
        indexer = VectorIndexer(store=mock_store)
        assert indexer.store is mock_store


def test_direct_import_check():
    """Check for direct MemgraphStore imports in ingestion service.

    This is an audit test to identify files that need refactoring.
    """
    ingestion_path = Path("src/ingestion")

    files_with_direct_imports = []
    for py_file in ingestion_path.glob("**/*.py"):
        content = py_file.read_text()
        if "from dal.memgraph import MemgraphStore" in content:
            files_with_direct_imports.append(str(py_file))

    # We expect these to be zero now that refactoring is complete
    assert (
        len(files_with_direct_imports) == 0
    ), f"Found direct imports in: {files_with_direct_imports}"
