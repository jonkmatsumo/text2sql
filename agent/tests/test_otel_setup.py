import os
import sys
from unittest.mock import MagicMock

import pytest

# Setup mocks for all required OTEL modules
# We must do this BEFORE importing agent_core.telemetry
otel_mock = MagicMock()
sys.modules["opentelemetry"] = otel_mock
sys.modules["opentelemetry.context"] = MagicMock()
sys.modules["opentelemetry.propagate"] = MagicMock()
sys.modules["opentelemetry.trace"] = MagicMock()
sys.modules["opentelemetry.sdk"] = MagicMock()
sys.modules["opentelemetry.sdk.resources"] = MagicMock()
sys.modules["opentelemetry.sdk.trace"] = MagicMock()
sys.modules["opentelemetry.sdk.trace.export"] = MagicMock()
# Mock the exporter modules that are dynamically imported
sys.modules["opentelemetry.exporter.otlp.proto.grpc.trace_exporter"] = MagicMock()
sys.modules["opentelemetry.exporter.otlp.proto.http.trace_exporter"] = MagicMock()

# Ensure that 'from opentelemetry import trace' gets the same mock as 'import opentelemetry.trace'
otel_mock.trace = sys.modules["opentelemetry.trace"]
otel_mock.context = sys.modules["opentelemetry.context"]
otel_mock.propagate = sys.modules["opentelemetry.propagate"]

# Now import the module under test
from agent_core import telemetry  # noqa: E402


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
    # Reset the global flag in telemetry module
    telemetry._otel_initialized = False
    yield
    telemetry._otel_initialized = False


@pytest.fixture(autouse=True)
def reset_mocks():
    """Reset all mocks before each test."""
    sys.modules["opentelemetry"].reset_mock()
    sys.modules["opentelemetry.context"].reset_mock()
    sys.modules["opentelemetry.propagate"].reset_mock()
    sys.modules["opentelemetry.trace"].reset_mock()
    sys.modules["opentelemetry.sdk"].reset_mock()
    sys.modules["opentelemetry.sdk.resources"].reset_mock()
    sys.modules["opentelemetry.sdk.trace"].reset_mock()
    sys.modules["opentelemetry.sdk.trace.export"].reset_mock()
    sys.modules["opentelemetry.exporter.otlp.proto.grpc.trace_exporter"].reset_mock()
    yield


def test_otel_setup(clean_env, reset_telemetry_globals):
    """Verify that OTEL backend configures the SDK."""
    os.environ["TELEMETRY_BACKEND"] = "otel"
    os.environ["OTEL_EXPORTER_OTLP_ENDPOINT"] = "http://localhost:4317"
    # Protocol defaults to grpc in code

    # Re-initialize service to pick up env var
    svc = telemetry.TelemetryService()

    # Get the mocks that should be used
    mock_resource_create = sys.modules["opentelemetry.sdk.resources"].Resource.create
    mock_provider_cls = sys.modules["opentelemetry.sdk.trace"].TracerProvider
    mock_grpc_exporter_cls = sys.modules[
        "opentelemetry.exporter.otlp.proto.grpc.trace_exporter"
    ].OTLPSpanExporter
    mock_processor_cls = sys.modules["opentelemetry.sdk.trace.export"].BatchSpanProcessor

    # Note: When importing trace from opentelemetry, and opentelemetry.trace is mocked,
    # the imported 'trace' object is sys.modules["opentelemetry.trace"]
    mock_set_provider = sys.modules["opentelemetry.trace"].set_tracer_provider

    # Act
    svc.configure()

    # Assert
    mock_resource_create.assert_called_once()
    mock_provider_cls.assert_called_once()
    mock_grpc_exporter_cls.assert_called_once_with(endpoint="http://localhost:4317")
    mock_processor_cls.assert_called_once()
    mock_set_provider.assert_called_once()


def test_mlflow_backend_does_not_configure_otel(clean_env, reset_telemetry_globals):
    """Verify that MLflow backend does NOT configure OTEL SDK."""
    os.environ["TELEMETRY_BACKEND"] = "mlflow"

    svc = telemetry.TelemetryService()

    # Mocks
    mock_resource_create = sys.modules["opentelemetry.sdk.resources"].Resource.create

    # Act
    svc.configure()

    # Assert
    mock_resource_create.assert_not_called()
