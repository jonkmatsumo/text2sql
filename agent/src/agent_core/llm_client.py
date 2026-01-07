"""LLM Client factory for multi-provider support.

Supports OpenAI, Anthropic (Claude), and Google (Gemini) with runtime model selection.
"""

import os
from typing import Optional

from dotenv import load_dotenv
from langchain_core.language_models.chat_models import BaseChatModel

load_dotenv()

# Default configuration
DEFAULT_PROVIDER = "openai"
DEFAULT_MODEL = "gpt-5.2"

# Supported models by provider
SUPPORTED_MODELS = {
    "openai": ["gpt-5.2", "gpt-5.1", "gpt-5", "gpt-4o", "gpt-4o-mini"],
    "anthropic": ["claude-sonnet-4-20250514", "claude-3-5-sonnet-20241022"],
    "google": ["gemini-2.5-flash-preview-05-20", "gemini-2.5-pro-preview-05-06"],
}


def get_llm_client(
    provider: Optional[str] = None,
    model: Optional[str] = None,
    temperature: float = 0,
) -> BaseChatModel:
    """Get an LLM client for the specified provider.

    Args:
        provider: LLM provider ('openai', 'anthropic', 'google').
                  Defaults to LLM_PROVIDER env var or 'openai'.
        model: Model name. Defaults to LLM_MODEL env var or provider default.
        temperature: Temperature for generation. Defaults to 0 (deterministic).

    Returns:
        BaseChatModel: LangChain chat model instance.

    Raises:
        ValueError: If provider is not supported.
    """
    # Resolve provider from env or default
    resolved_provider = provider or os.getenv("LLM_PROVIDER", DEFAULT_PROVIDER)
    resolved_provider = resolved_provider.lower()

    # Validate provider
    if resolved_provider not in SUPPORTED_MODELS:
        raise ValueError(
            f"Unsupported provider: {resolved_provider}. "
            f"Supported: {list(SUPPORTED_MODELS.keys())}"
        )

    # Resolve model from env or default
    resolved_model = model or os.getenv("LLM_MODEL", DEFAULT_MODEL)

    # Create client based on provider
    if resolved_provider == "openai":
        from langchain_openai import ChatOpenAI

        return ChatOpenAI(model=resolved_model, temperature=temperature)

    elif resolved_provider == "anthropic":
        from langchain_anthropic import ChatAnthropic

        return ChatAnthropic(model=resolved_model, temperature=temperature)

    elif resolved_provider == "google":
        from langchain_google_genai import ChatGoogleGenerativeAI

        return ChatGoogleGenerativeAI(model=resolved_model, temperature=temperature)

    # Should not reach here due to validation above
    raise ValueError(f"Unsupported provider: {resolved_provider}")


def get_available_models(provider: str) -> list[str]:
    """
    Get list of available models for a provider.

    Args:
        provider: LLM provider name.

    Returns:
        List of model names, or empty list if provider not found.
    """
    return SUPPORTED_MODELS.get(provider.lower(), [])


def get_available_providers() -> list[str]:
    """
    Get list of available providers.

    Returns:
        List of provider names.
    """
    return list(SUPPORTED_MODELS.keys())
