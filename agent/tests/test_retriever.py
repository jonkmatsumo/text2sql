"""Unit tests for vector store retriever."""

import os
import sys
from types import ModuleType
from unittest.mock import MagicMock, patch

if "langchain_postgres" not in sys.modules:
    mock_pkg = ModuleType("langchain_postgres")
    mock_pkg.PGVector = MagicMock()
    sys.modules["langchain_postgres"] = mock_pkg

from agent_core.retriever import get_vector_store


class TestGetVectorStore:
    """Unit tests for get_vector_store function."""

    @patch("agent_core.retriever.PGVector")
    @patch("agent_core.retriever.OpenAIEmbeddings")
    @patch.dict(os.environ, {}, clear=True)
    def test_get_vector_store_success(self, mock_openai, mock_pgvector):
        """Test successful vector store initialization."""
        mock_store = MagicMock()
        mock_pgvector.return_value = mock_store
        mock_embeddings = MagicMock()
        mock_openai.return_value = mock_embeddings

        # Set environment variables
        os.environ["DB_HOST"] = "localhost"
        os.environ["DB_PORT"] = "5432"
        os.environ["DB_NAME"] = "pagila"
        os.environ["DB_USER"] = "postgres"
        os.environ["DB_PASSWORD"] = "test_password"

        result = get_vector_store()

        # Verify OpenAI embeddings were created with correct model
        mock_openai.assert_called_once_with(model="text-embedding-3-small")

        # Verify PGVector was created with correct parameters
        mock_pgvector.assert_called_once()
        call_kwargs = mock_pgvector.call_args[1]
        assert call_kwargs["embeddings"] == mock_embeddings
        assert call_kwargs["collection_name"] == "schema_metadata"
        assert call_kwargs["use_jsonb"] is True
        assert (
            "postgresql://postgres:test_password@localhost:5432/pagila" in call_kwargs["connection"]
        )

        # Verify result is the mock store
        assert result == mock_store

    @patch("agent_core.retriever.PGVector")
    @patch("agent_core.retriever.OpenAIEmbeddings")
    @patch.dict(os.environ, {}, clear=True)
    def test_get_vector_store_connection_string(self, mock_openai, mock_pgvector):
        """Test connection string format."""
        mock_store = MagicMock()
        mock_pgvector.return_value = mock_store
        mock_openai.return_value = MagicMock()

        # Set custom environment variables
        os.environ["DB_HOST"] = "db.example.com"
        os.environ["DB_PORT"] = "5433"
        os.environ["DB_NAME"] = "test_db"
        os.environ["DB_USER"] = "test_user"
        os.environ["DB_PASSWORD"] = "secret_pass"

        get_vector_store()

        # Verify connection string format
        call_kwargs = mock_pgvector.call_args[1]
        expected_connection = "postgresql://test_user:secret_pass@db.example.com:5433/test_db"
        assert call_kwargs["connection"] == expected_connection

    @patch("agent_core.retriever.PGVector")
    @patch("agent_core.retriever.OpenAIEmbeddings")
    @patch.dict(os.environ, {}, clear=True)
    def test_get_vector_store_env_vars(self, mock_openai, mock_pgvector):
        """Test environment variable loading."""
        mock_store = MagicMock()
        mock_pgvector.return_value = mock_store
        mock_openai.return_value = MagicMock()

        # Test with environment variables set
        os.environ["DB_HOST"] = "custom_host"
        os.environ["DB_PORT"] = "9999"
        os.environ["DB_NAME"] = "custom_db"
        os.environ["DB_USER"] = "custom_user"
        os.environ["DB_PASSWORD"] = "custom_pass"

        get_vector_store()

        # Verify environment variables were used
        call_kwargs = mock_pgvector.call_args[1]
        assert "custom_host" in call_kwargs["connection"]
        assert "9999" in call_kwargs["connection"]
        assert "custom_db" in call_kwargs["connection"]
        assert "custom_user" in call_kwargs["connection"]
        assert "custom_pass" in call_kwargs["connection"]

    @patch("agent_core.retriever.PGVector")
    @patch("agent_core.retriever.OpenAIEmbeddings")
    @patch.dict(os.environ, {}, clear=True)
    def test_get_vector_store_defaults(self, mock_openai, mock_pgvector):
        """Test default values when environment variables are not set."""
        mock_store = MagicMock()
        mock_pgvector.return_value = mock_store
        mock_openai.return_value = MagicMock()

        # Don't set any environment variables
        get_vector_store()

        # Verify defaults were used
        call_kwargs = mock_pgvector.call_args[1]
        expected_connection = "postgresql://text2sql_ro:secure_agent_pass@localhost:5432/pagila"
        assert call_kwargs["connection"] == expected_connection

    @patch("agent_core.retriever.PGVector")
    @patch("agent_core.retriever.OpenAIEmbeddings")
    @patch.dict(os.environ, {}, clear=True)
    def test_get_vector_store_openai_embeddings(self, mock_openai, mock_pgvector):
        """Test OpenAI embeddings model configuration."""
        mock_store = MagicMock()
        mock_pgvector.return_value = mock_store
        mock_embeddings = MagicMock()
        mock_openai.return_value = mock_embeddings

        os.environ["DB_HOST"] = "localhost"

        get_vector_store()

        # Verify OpenAI embeddings were created with correct model
        mock_openai.assert_called_once_with(model="text-embedding-3-small")

        # Verify embeddings were passed to PGVector
        call_kwargs = mock_pgvector.call_args[1]
        assert call_kwargs["embeddings"] == mock_embeddings

    @patch("agent_core.retriever.PGVector")
    @patch("agent_core.retriever.OpenAIEmbeddings")
    @patch.dict(os.environ, {}, clear=True)
    def test_get_vector_store_collection_name(self, mock_openai, mock_pgvector):
        """Test collection name configuration."""
        mock_store = MagicMock()
        mock_pgvector.return_value = mock_store
        mock_openai.return_value = MagicMock()

        os.environ["DB_HOST"] = "localhost"

        get_vector_store()

        # Verify collection name
        call_kwargs = mock_pgvector.call_args[1]
        assert call_kwargs["collection_name"] == "schema_metadata"

    @patch("agent_core.retriever.PGVector")
    @patch("agent_core.retriever.OpenAIEmbeddings")
    @patch.dict(os.environ, {}, clear=True)
    def test_get_vector_store_use_jsonb(self, mock_openai, mock_pgvector):
        """Test jsonb configuration."""
        mock_store = MagicMock()
        mock_pgvector.return_value = mock_store
        mock_openai.return_value = MagicMock()

        os.environ["DB_HOST"] = "localhost"

        get_vector_store()

        # Verify use_jsonb setting
        call_kwargs = mock_pgvector.call_args[1]
        assert call_kwargs["use_jsonb"] is True

    @patch("agent_core.retriever.PGVector")
    @patch("agent_core.retriever.OpenAIEmbeddings")
    @patch.dict(os.environ, {}, clear=True)
    def test_get_vector_store_partial_env_vars(self, mock_openai, mock_pgvector):
        """Test with some environment variables set and others using defaults."""
        mock_store = MagicMock()
        mock_pgvector.return_value = mock_store
        mock_openai.return_value = MagicMock()

        # Set only some environment variables
        os.environ["DB_HOST"] = "custom_host"
        os.environ["DB_NAME"] = "custom_db"
        # DB_PORT, DB_USER, DB_PASSWORD should use defaults

        get_vector_store()

        # Verify mix of custom and default values
        call_kwargs = mock_pgvector.call_args[1]
        connection = call_kwargs["connection"]
        assert "custom_host" in connection
        assert "custom_db" in connection
        assert "5432" in connection  # default port
        assert "text2sql_ro" in connection  # default user
        assert "secure_agent_pass" in connection  # default password

    @patch("agent_core.retriever.PGVector")
    @patch("agent_core.retriever.OpenAIEmbeddings")
    @patch.dict(os.environ, {}, clear=True)
    def test_get_vector_store_synthetic_db_name(self, mock_openai, mock_pgvector):
        """Test with synthetic data DB name (non-pagila)."""
        mock_store = MagicMock()
        mock_pgvector.return_value = mock_store
        mock_openai.return_value = MagicMock()

        # Use synthetic financial data database
        os.environ["DB_NAME"] = "synth_financial"
        os.environ["DB_HOST"] = "localhost"

        get_vector_store()

        # Verify synthetic DB name is used
        call_kwargs = mock_pgvector.call_args[1]
        assert "synth_financial" in call_kwargs["connection"]
        assert "pagila" not in call_kwargs["connection"]
