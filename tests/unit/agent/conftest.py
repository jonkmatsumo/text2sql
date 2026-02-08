import os
import sys
from unittest.mock import MagicMock

import pytest

from tests._support.fixtures.schema_fixtures import SYNTHETIC_FIXTURE

# Mock mcp if not available (for CI/local envs without mcp installed)
try:
    import mcp  # noqa: F401
except ImportError:
    import types

    mcp_mock = types.ModuleType("mcp")
    mcp_mock.__spec__ = MagicMock()
    mcp_mock.__path__ = []
    sys.modules["mcp"] = mcp_mock
    sys.modules["mcp_server.types"] = MagicMock()
    sys.modules["mcp_server.server"] = MagicMock()

try:
    import langchain_mcp_adapters  # noqa: F401
except ImportError:
    sys.modules["langchain_mcp_adapters"] = MagicMock()
    sys.modules["langchain_mcp_adapters.client"] = MagicMock()


@pytest.fixture
def clean_env():
    """Clean environment variables after test."""
    old_env = os.environ.copy()
    yield
    os.environ.clear()
    os.environ.update(old_env)


@pytest.fixture(autouse=True)
def autoreset_globals():
    """Reset all global state before each test to ensure isolation."""
    # 1. Telemetry
    from agent import telemetry

    telemetry._otel_initialized = False

    # 2. Prefetch
    from agent.utils.pagination_prefetch import reset_prefetch_state

    reset_prefetch_state()

    # 3. Cache Lookup
    from agent.nodes.cache_lookup import reset_cache_state

    reset_cache_state()

    # 4. Schema Cache
    from agent.utils.schema_cache import reset_schema_cache

    reset_schema_cache()

    yield

    # Optional: cleanup after as well
    reset_prefetch_state()
    reset_cache_state()
    reset_schema_cache()


@pytest.fixture(scope="session")
def dataset_mode():
    """Return the current dataset mode from env (default: synthetic)."""
    from common.config.dataset import get_dataset_mode

    return get_dataset_mode()


def _get_default_schema_fixtures():
    """Return schema fixtures (Synthetic only)."""
    return [SYNTHETIC_FIXTURE]


@pytest.fixture(params=_get_default_schema_fixtures(), ids=lambda f: f.name)
def schema_fixture(request):
    """Fixture providing dataset-specific schema details.

    Default is Synthetic.
    """
    return request.param
