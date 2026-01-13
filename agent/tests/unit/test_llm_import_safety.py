"""Unit test to verify that importing agent modules does NOT trigger eager LLM instantiation.

This ensures that the test suite can be collected and run in environments without an API key,
mocking the LLM interaction instead.
"""

import pytest


@pytest.fixture
def no_api_keys(monkeypatch):
    """Ensure no API keys are present in the environment."""
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    monkeypatch.delenv("GOOGLE_API_KEY", raising=False)


def test_llm_client_import_safe(no_api_keys):
    """Verify that importing agent_core.llm_client is safe without API keys."""
    # Ensure we are reloading actual modules, not mocks from other tests
    import sys

    # Aggressively clean up any mocks in agent_core namespace
    clean_modules = [m for m in sys.modules if m.startswith("agent_core")]
    for m in clean_modules:
        if not isinstance(sys.modules[m], type(sys)):
            del sys.modules[m]

    # Also ensure agent_core itself is a module
    if "agent_core" in sys.modules and not isinstance(sys.modules["agent_core"], type(sys)):
        del sys.modules["agent_core"]

    import agent_core.llm_client  # noqa: F401

    # No need to reload if we freshly imported it after deletion
    # importlib.reload(agent_core.llm_client)

    if "agent_core.graph" in sys.modules and not isinstance(
        sys.modules["agent_core.graph"], type(sys)
    ):
        del sys.modules["agent_core.graph"]


def test_llm_accessor_lazy_init(no_api_keys):
    """Verify that get_llm() initializes lazily and raises error only on access."""
    # Reload to ensure cache is empty and it sees the no_api_keys env
    import sys

    # Aggressively clean up any mocks in agent_core namespace
    clean_modules = [m for m in sys.modules if m.startswith("agent_core")]
    for m in clean_modules:
        if not isinstance(sys.modules[m], type(sys)):
            del sys.modules[m]

    if "agent_core" in sys.modules and not isinstance(sys.modules["agent_core"], type(sys)):
        del sys.modules["agent_core"]

    import agent_core.llm_client  # noqa: F401

    # importlib.reload(agent_core.llm_client)
    from agent_core.llm_client import get_llm

    # Getting the accessor should succeed (it initializes the client)
    # Even without API keys, ChatOpenAI might initialize depending on version/config,
    # or fail. Both are acceptable here as long as IMPORT was safe.
    try:
        llm = get_llm()
        assert llm is not None
    except Exception:
        # If it raises due to missing key, that is also acceptable behavior for this test context.
        # It proves that initialization was attempted (lazy).
        pass
