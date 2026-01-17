import os

import pytest

from agent.tests.schema_fixtures import PAGILA_FIXTURE


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


@pytest.fixture(params=[PAGILA_FIXTURE], ids=lambda f: f.name)
def schema_fixture(request):
    """Fixture providing dataset-specific schema details.

    Default is Pagila for backward compatibility.
    Use @pytest.mark.parametrize(
        "schema_fixture", [PAGILA_FIXTURE, SYNTHETIC_FIXTURE], indirect=True
    )
    to test against other datasets.
    """
    return request.param
