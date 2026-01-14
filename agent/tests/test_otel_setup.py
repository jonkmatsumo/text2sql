import os
from unittest.mock import MagicMock, patch

import pytest


@pytest.fixture
def otel_mocks():
    """Mock all required OTEL modules and inject into sys.modules."""
    otel_mock = MagicMock()

    # Define the mocks we want to inject
    mocks = {
        "opentelemetry": otel_mock,
        "opentelemetry.context": MagicMock(),
        "opentelemetry.propagate": MagicMock(),
        "opentelemetry.trace": MagicMock(),
        "opentelemetry.sdk": MagicMock(),
        "opentelemetry.sdk.resources": MagicMock(),
        "opentelemetry.sdk.trace": MagicMock(),
        "opentelemetry.sdk.trace.export": MagicMock(),
        "opentelemetry.exporter.otlp.proto.grpc.trace_exporter": MagicMock(),
        "opentelemetry.exporter.otlp.proto.http.trace_exporter": MagicMock(),
    }

    # Setup internal attributes of the main mock
    otel_mock.trace = mocks["opentelemetry.trace"]
    otel_mock.context = mocks["opentelemetry.context"]
    otel_mock.propagate = mocks["opentelemetry.propagate"]

    with patch.dict("sys.modules", mocks), patch(
        "agent_core.telemetry.Resource", mocks["opentelemetry.sdk.resources"].Resource
    ), patch(
        "agent_core.telemetry.TracerProvider", mocks["opentelemetry.sdk.trace"].TracerProvider
    ), patch(
        "agent_core.telemetry.BatchSpanProcessor",
        mocks["opentelemetry.sdk.trace.export"].BatchSpanProcessor,
    ), patch(
        "agent_core.telemetry.trace", mocks["opentelemetry.trace"]
    ):

        from agent_core import telemetry

        # Ensure the global flag is reset so configure() actually runs logic
        telemetry._otel_initialized = False

        yield mocks, telemetry


def test_otel_setup(clean_env, reset_telemetry_globals, otel_mocks):
    """Verify that OTEL backend configures the SDK."""
    os.environ["TELEMETRY_BACKEND"] = "otel"
    os.environ["OTEL_EXPORTER_OTLP_ENDPOINT"] = "http://localhost:4317"

    mocks, telemetry = otel_mocks
    svc = telemetry.TelemetryService()

    # Get the mocks that should be used
    mock_resource_create = mocks["opentelemetry.sdk.resources"].Resource.create
    mock_provider_cls = mocks["opentelemetry.sdk.trace"].TracerProvider
    mock_grpc_exporter_cls = mocks[
        "opentelemetry.exporter.otlp.proto.grpc.trace_exporter"
    ].OTLPSpanExporter
    mock_processor_cls = mocks["opentelemetry.sdk.trace.export"].BatchSpanProcessor

    mock_set_provider = mocks["opentelemetry.trace"].set_tracer_provider

    # Act
    svc.configure()

    # Assert
    mock_resource_create.assert_called_once()
    mock_provider_cls.assert_called_once()
    mock_grpc_exporter_cls.assert_called_once_with(endpoint="http://localhost:4317")
    mock_processor_cls.assert_called_once()
    mock_set_provider.assert_called_once()


def test_mlflow_backend_does_not_configure_otel(clean_env, reset_telemetry_globals, otel_mocks):
    """Verify that MLflow backend does NOT configure OTEL SDK."""
    os.environ["TELEMETRY_BACKEND"] = "mlflow"

    mocks, telemetry = otel_mocks
    svc = telemetry.TelemetryService()

    # Mocks
    mock_resource_create = mocks["opentelemetry.sdk.resources"].Resource.create

    # Act
    svc.configure()

    # Assert
    mock_resource_create.assert_not_called()
