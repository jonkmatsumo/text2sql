import os
import sys
from unittest.mock import MagicMock

import pytest

from tests._support.fixtures.schema_fixtures import PAGILA_FIXTURE, SYNTHETIC_FIXTURE

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
    """Return schema fixtures based on DATASET_MODE and manual override.

    Default: SYNTHETIC_FIXTURE only
    DATASET_MODE=pagila or RUN_PAGILA_TESTS=1: Include PAGILA_FIXTURE
    """
    from common.config.dataset import get_dataset_mode

    if os.getenv("RUN_PAGILA_TESTS", "0") == "1" or get_dataset_mode() == "pagila":
        return [PAGILA_FIXTURE, SYNTHETIC_FIXTURE]
    return [SYNTHETIC_FIXTURE]


@pytest.fixture(params=_get_default_schema_fixtures(), ids=lambda f: f.name)
def schema_fixture(request):
    """Fixture providing dataset-specific schema details.

    Default is Synthetic. Set DATASET_MODE=pagila to include Pagila tests.
    """
    return request.param


def pytest_collection_modifyitems(config, items):
    """Skip pagila-marked tests unless DATASET_MODE=pagila or RUN_PAGILA_TESTS=1."""
    from common.config.dataset import get_dataset_mode

    should_run_pagila = os.getenv("RUN_PAGILA_TESTS", "0") == "1" or get_dataset_mode() == "pagila"

    if should_run_pagila:
        return

    skip_pagila = pytest.mark.skip(
        reason="Skipping pagila dataset test (set DATASET_MODE=pagila to run)"
    )
    for item in items:
        if "pagila" in item.keywords or "dataset_pagila" in item.keywords:
            item.add_marker(skip_pagila)
