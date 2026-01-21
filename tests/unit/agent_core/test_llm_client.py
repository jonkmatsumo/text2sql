"""Unit tests for LLM client factory."""

import os
import sys
from unittest.mock import MagicMock, patch

import pytest

# Check if optional dependencies are available
HAS_ANTHROPIC = (
    "langchain_anthropic" in sys.modules
    or __import__("importlib.util").util.find_spec("langchain_anthropic") is not None
)
HAS_GOOGLE = (
    "langchain_google_genai" in sys.modules
    or __import__("importlib.util").util.find_spec("langchain_google_genai") is not None
)


class TestGetLLMClient:
    """Unit tests for get_llm_client factory function."""

    @patch.dict(os.environ, {"OPENAI_API_KEY": "sk-test-key"}, clear=True)
    @patch("langchain_openai.ChatOpenAI")
    def test_get_llm_client_openai_default(self, mock_openai):
        """Test that OpenAI with gpt-5.2 is returned when no env vars set."""
        from agent_core.llm_client import get_llm_client

        mock_openai.return_value = MagicMock()

        result = get_llm_client()

        mock_openai.assert_called_once_with(model="gpt-5.2", temperature=0)
        assert result == mock_openai.return_value

    @patch.dict(os.environ, {"LLM_MODEL": "gpt-4o", "OPENAI_API_KEY": "sk-test-key"}, clear=True)
    @patch("langchain_openai.ChatOpenAI")
    def test_get_llm_client_openai_custom_model(self, mock_openai):
        """Test that custom model from env var is used."""
        from agent_core.llm_client import get_llm_client

        mock_openai.return_value = MagicMock()

        result = get_llm_client()

        mock_openai.assert_called_once_with(model="gpt-4o", temperature=0)
        assert result == mock_openai.return_value

    @pytest.mark.skipif(not HAS_ANTHROPIC, reason="langchain-anthropic not installed")
    @patch.dict(os.environ, {"LLM_PROVIDER": "anthropic"}, clear=True)
    @patch("langchain_anthropic.ChatAnthropic")
    def test_get_llm_client_anthropic(self, mock_anthropic):
        """Test that ChatAnthropic is returned for anthropic provider."""
        from agent_core.llm_client import get_llm_client

        mock_anthropic.return_value = MagicMock()

        result = get_llm_client()

        mock_anthropic.assert_called_once_with(model="gpt-5.2", temperature=0)
        assert result == mock_anthropic.return_value

    @pytest.mark.skipif(not HAS_GOOGLE, reason="langchain-google-genai not installed")
    @patch.dict(os.environ, {"LLM_PROVIDER": "google"}, clear=True)
    @patch("langchain_google_genai.ChatGoogleGenerativeAI")
    def test_get_llm_client_google(self, mock_google):
        """Test that ChatGoogleGenerativeAI is returned for google provider."""
        from agent_core.llm_client import get_llm_client

        mock_google.return_value = MagicMock()

        result = get_llm_client()

        mock_google.assert_called_once_with(model="gpt-5.2", temperature=0)
        assert result == mock_google.return_value

    @patch.dict(os.environ, {"LLM_PROVIDER": "unsupported"}, clear=True)
    def test_get_llm_client_unsupported_provider(self):
        """Test that ValueError is raised for unsupported provider."""
        from agent_core.llm_client import get_llm_client

        with pytest.raises(ValueError, match="Unsupported provider: unsupported"):
            get_llm_client()

    @patch.dict(os.environ, {"OPENAI_API_KEY": "sk-test-key"}, clear=True)
    @patch("langchain_openai.ChatOpenAI")
    def test_get_llm_client_temperature(self, mock_openai):
        """Test that temperature is correctly passed to client."""
        from agent_core.llm_client import get_llm_client

        mock_openai.return_value = MagicMock()

        get_llm_client(temperature=0.7)

        mock_openai.assert_called_once_with(model="gpt-5.2", temperature=0.7)

    @patch.dict(
        os.environ,
        {"LLM_PROVIDER": "openai", "LLM_MODEL": "gpt-4o", "OPENAI_API_KEY": "sk-test-key"},
        clear=True,
    )
    @patch("langchain_openai.ChatOpenAI")
    def test_get_llm_client_override_params(self, mock_openai):
        """Test that explicit params override env vars."""
        from agent_core.llm_client import get_llm_client

        mock_openai.return_value = MagicMock()

        get_llm_client(provider="openai", model="gpt-5.1")

        mock_openai.assert_called_once_with(model="gpt-5.1", temperature=0)


class TestGetAvailableModels:
    """Unit tests for get_available_models function."""

    def test_get_available_models_openai(self):
        """Test that OpenAI models are returned."""
        from agent_core.llm_client import get_available_models

        models = get_available_models("openai")

        assert "gpt-5.2" in models
        assert "gpt-4o" in models

    def test_get_available_models_anthropic(self):
        """Test that Anthropic models are returned."""
        from agent_core.llm_client import get_available_models

        models = get_available_models("anthropic")

        assert "claude-sonnet-4-20250514" in models

    def test_get_available_models_google(self):
        """Test that Google models are returned."""
        from agent_core.llm_client import get_available_models

        models = get_available_models("google")

        assert "gemini-2.5-flash-preview-05-20" in models

    def test_get_available_models_unknown(self):
        """Test that empty list is returned for unknown provider."""
        from agent_core.llm_client import get_available_models

        models = get_available_models("unknown")

        assert models == []


class TestGetAvailableProviders:
    """Unit tests for get_available_providers function."""

    def test_get_available_providers(self):
        """Test that all providers are returned."""
        from agent_core.llm_client import get_available_providers

        providers = get_available_providers()

        assert "openai" in providers
        assert "anthropic" in providers
        assert "google" in providers
