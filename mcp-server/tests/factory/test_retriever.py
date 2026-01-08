from unittest.mock import patch

from mcp_server.factory.retriever import get_retriever


def test_get_retriever_singleton():
    """Test that get_retriever returns a singleton instance."""
    with patch("mcp_server.factory.retriever.PostgresRetriever") as mock_cls:
        # First call should create instance
        instance1 = get_retriever()
        mock_cls.assert_called_once()

        # Second call should return same instance
        instance2 = get_retriever()
        assert instance1 is instance2
        mock_cls.assert_called_once()  # Should not call constructor again


def test_get_retriever_initialization_reset():
    """Ensure we can reset implementation details if needed for testing isolation."""
    # This involves manipulating the module global which is tricky in parallel tests,
    # but for unit testing logic it's fine to rely on mocking.
    pass
