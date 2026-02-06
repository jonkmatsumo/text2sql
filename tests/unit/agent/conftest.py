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


@pytest.fixture
def reset_telemetry_globals():
    """Reset global state in telemetry module."""
    from agent import telemetry

    # Reset the global flag in telemetry module
    telemetry._otel_initialized = False
    yield
    telemetry._otel_initialized = False


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
