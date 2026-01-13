"""Unit test to verify that importing agent modules does NOT trigger eager LLM instantiation.

This ensures that the test suite can be collected and run in environments without an API key,
mocking the LLM interaction instead.
"""

import importlib
import sys

import pytest


@pytest.fixture
def no_api_keys(monkeypatch):
    """Ensure no API keys are present in the environment."""
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    monkeypatch.delenv("GOOGLE_API_KEY", raising=False)


def test_llm_client_import_safe(no_api_keys):
    """Verify that importing agent_core.llm_client is safe without API keys."""
    # Ensure it's not already imported
    if "agent_core.llm_client" in sys.modules:
        importlib.reload(sys.modules["agent_core.llm_client"])
    else:
        import agent_core.llm_client  # noqa: F401


def test_llm_accessor_lazy_init(no_api_keys):
    """Verify that get_llm() initializes lazily and raises error only on access."""
    from agent_core.llm_client import get_llm

    # Getting the accessor should NOT raise error yet.
    # The actual instantiation typically happens when the client is created.
    # If get_llm is lazy, it should call get_llm_client only when invoked.

    with pytest.raises(Exception):
        # This confirms that logic IS running when we request it,
        # proving we aren't just getting None.
        get_llm()
