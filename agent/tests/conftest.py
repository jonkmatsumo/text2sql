import os

import pytest

from agent.tests.schema_fixtures import PAGILA_FIXTURE, SYNTHETIC_FIXTURE


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
    from agent_core import telemetry

    # Reset the global flag in telemetry module
    telemetry._otel_initialized = False
    yield
    telemetry._otel_initialized = False


def _get_default_schema_fixtures():
    """Return schema fixtures based on RUN_PAGILA_TESTS env var.

    Default: SYNTHETIC_FIXTURE only
    RUN_PAGILA_TESTS=1: Include both fixtures
    """
    if os.getenv("RUN_PAGILA_TESTS", "0") == "1":
        return [SYNTHETIC_FIXTURE, PAGILA_FIXTURE]
    return [SYNTHETIC_FIXTURE]


@pytest.fixture(params=_get_default_schema_fixtures(), ids=lambda f: f.name)
def schema_fixture(request):
    """Fixture providing dataset-specific schema details.

    Default is Synthetic. Set RUN_PAGILA_TESTS=1 to include Pagila tests.
    Use @pytest.mark.parametrize(
        "schema_fixture", [PAGILA_FIXTURE, SYNTHETIC_FIXTURE], indirect=True
    ) to explicitly test against both datasets.
    """
    return request.param


def pytest_collection_modifyitems(config, items):
    """Skip pagila-marked tests unless RUN_PAGILA_TESTS=1."""
    if os.getenv("RUN_PAGILA_TESTS", "0") == "1":
        return

    skip_pagila = pytest.mark.skip(
        reason="Skipping pagila dataset test (set RUN_PAGILA_TESTS=1 to run)"
    )
    for item in items:
        if "dataset_pagila" in item.keywords:
            item.add_marker(skip_pagila)
