import os

import pytest


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
