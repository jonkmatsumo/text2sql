"""Unit tests for context retrieval node."""

from unittest.mock import MagicMock, patch

import pytest
from src.nodes.retrieve import retrieve_context_node
from src.state import AgentState


class TestRetrieveContextNode:
    """Unit tests for retrieve_context_node function."""

    @patch("src.nodes.retrieve.get_vector_store")
    def test_retrieve_context_node_success(self, mock_get_vector_store):
        """Test successful context retrieval."""
        # Create mock vector store and documents
        mock_store = MagicMock()
        mock_doc1 = MagicMock()
        mock_doc1.page_content = "Table: customer. Columns: id, name"
        mock_doc2 = MagicMock()
        mock_doc2.page_content = "Table: payment. Columns: amount, customer_id"
        mock_store.similarity_search.return_value = [mock_doc1, mock_doc2]
        mock_get_vector_store.return_value = mock_store

        # Create test state
        from langchain_core.messages import HumanMessage

        state = AgentState(
            messages=[HumanMessage(content="Show me customer payments")],
            schema_context="",
            current_sql=None,
            query_result=None,
            error=None,
            retry_count=0,
        )

        result = retrieve_context_node(state)

        # Verify vector store was called
        mock_get_vector_store.assert_called_once()

        # Verify similarity search was called with correct query and k
        mock_store.similarity_search.assert_called_once_with("Show me customer payments", k=5)

        # Verify context was formatted correctly
        assert "schema_context" in result
        assert "Table: customer" in result["schema_context"]
        assert "Table: payment" in result["schema_context"]
        assert "\n\n" in result["schema_context"]

    @patch("src.nodes.retrieve.get_vector_store")
    def test_retrieve_context_node_extracts_query(self, mock_get_vector_store):
        """Test that query is extracted from last message."""
        mock_store = MagicMock()
        mock_store.similarity_search.return_value = []
        mock_get_vector_store.return_value = mock_store

        from langchain_core.messages import HumanMessage

        test_query = "Find all actors in action movies"
        state = AgentState(
            messages=[HumanMessage(content=test_query)],
            schema_context="",
            current_sql=None,
            query_result=None,
            error=None,
            retry_count=0,
        )

        retrieve_context_node(state)

        # Verify similarity search was called with the extracted query
        mock_store.similarity_search.assert_called_once_with(test_query, k=5)

    @patch("src.nodes.retrieve.get_vector_store")
    def test_retrieve_context_node_top_k(self, mock_get_vector_store):
        """Test that k=5 is used for similarity search."""
        mock_store = MagicMock()
        mock_store.similarity_search.return_value = []
        mock_get_vector_store.return_value = mock_store

        from langchain_core.messages import HumanMessage

        state = AgentState(
            messages=[HumanMessage(content="test query")],
            schema_context="",
            current_sql=None,
            query_result=None,
            error=None,
            retry_count=0,
        )

        retrieve_context_node(state)

        # Verify k=5 was used
        mock_store.similarity_search.assert_called_once_with("test query", k=5)

    @patch("src.nodes.retrieve.get_vector_store")
    def test_retrieve_context_node_formatting(self, mock_get_vector_store):
        """Test context string formatting."""
        mock_store = MagicMock()
        mock_doc1 = MagicMock()
        mock_doc1.page_content = "Schema 1"
        mock_doc2 = MagicMock()
        mock_doc2.page_content = "Schema 2"
        mock_doc3 = MagicMock()
        mock_doc3.page_content = "Schema 3"
        mock_store.similarity_search.return_value = [mock_doc1, mock_doc2, mock_doc3]
        mock_get_vector_store.return_value = mock_store

        from langchain_core.messages import HumanMessage

        state = AgentState(
            messages=[HumanMessage(content="test")],
            schema_context="",
            current_sql=None,
            query_result=None,
            error=None,
            retry_count=0,
        )

        result = retrieve_context_node(state)

        # Verify formatting with double newline separator
        expected = "Schema 1\n\nSchema 2\n\nSchema 3"
        assert result["schema_context"] == expected

    @patch("src.nodes.retrieve.get_vector_store")
    def test_retrieve_context_node_empty_results(self, mock_get_vector_store):
        """Test handling of empty search results."""
        mock_store = MagicMock()
        mock_store.similarity_search.return_value = []
        mock_get_vector_store.return_value = mock_store

        from langchain_core.messages import HumanMessage

        state = AgentState(
            messages=[HumanMessage(content="test query")],
            schema_context="",
            current_sql=None,
            query_result=None,
            error=None,
            retry_count=0,
        )

        result = retrieve_context_node(state)

        # Verify empty context string is returned
        assert result["schema_context"] == ""

    @patch("src.nodes.retrieve.get_vector_store")
    def test_retrieve_context_node_multiple_messages(self, mock_get_vector_store):
        """Test that query is extracted from the last message when multiple messages exist."""
        mock_store = MagicMock()
        mock_store.similarity_search.return_value = []
        mock_get_vector_store.return_value = mock_store

        from langchain_core.messages import AIMessage, HumanMessage

        # Create state with multiple messages
        state = AgentState(
            messages=[
                HumanMessage(content="First query"),
                AIMessage(content="Response"),
                HumanMessage(content="Second query"),
            ],
            schema_context="",
            current_sql=None,
            query_result=None,
            error=None,
            retry_count=0,
        )

        retrieve_context_node(state)

        # Verify last message content was used
        mock_store.similarity_search.assert_called_once_with("Second query", k=5)

    @patch("src.nodes.retrieve.get_vector_store")
    def test_retrieve_context_node_error_handling(self, mock_get_vector_store):
        """Test error handling when vector store fails."""
        mock_store = MagicMock()
        mock_store.similarity_search.side_effect = Exception("Vector store error")
        mock_get_vector_store.return_value = mock_store

        from langchain_core.messages import HumanMessage

        state = AgentState(
            messages=[HumanMessage(content="test query")],
            schema_context="",
            current_sql=None,
            query_result=None,
            error=None,
            retry_count=0,
        )

        # Verify exception is raised
        with pytest.raises(Exception, match="Vector store error"):
            retrieve_context_node(state)

    @patch("src.nodes.retrieve.get_vector_store")
    def test_retrieve_context_node_single_result(self, mock_get_vector_store):
        """Test formatting with single result."""
        mock_store = MagicMock()
        mock_doc = MagicMock()
        mock_doc.page_content = "Single schema"
        mock_store.similarity_search.return_value = [mock_doc]
        mock_get_vector_store.return_value = mock_store

        from langchain_core.messages import HumanMessage

        state = AgentState(
            messages=[HumanMessage(content="test")],
            schema_context="",
            current_sql=None,
            query_result=None,
            error=None,
            retry_count=0,
        )

        result = retrieve_context_node(state)

        # Verify single result is returned without separators
        assert result["schema_context"] == "Single schema"

    @patch("src.nodes.retrieve.get_vector_store")
    def test_retrieve_context_node_max_results(self, mock_get_vector_store):
        """Test that exactly k=5 results are returned when available."""
        mock_store = MagicMock()
        # Create 5 mock documents
        mock_docs = [MagicMock() for _ in range(5)]
        for i, doc in enumerate(mock_docs):
            doc.page_content = f"Schema {i + 1}"
        mock_store.similarity_search.return_value = mock_docs
        mock_get_vector_store.return_value = mock_store

        from langchain_core.messages import HumanMessage

        state = AgentState(
            messages=[HumanMessage(content="test")],
            schema_context="",
            current_sql=None,
            query_result=None,
            error=None,
            retry_count=0,
        )

        result = retrieve_context_node(state)

        # Verify all 5 results are included
        assert len(result["schema_context"].split("\n\n")) == 5
        for i in range(5):
            assert f"Schema {i + 1}" in result["schema_context"]
