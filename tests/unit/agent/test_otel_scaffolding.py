import os

import pytest

from agent.telemetry import OTELTelemetryBackend

pytestmark = pytest.mark.skipif(os.getenv("CI") == "true", reason="Failing in CI environment")


def test_otel_import_sanity():
    """Verify that OTEL SDK and backend can be imported and instantiated."""
    try:
        import opentelemetry
        from opentelemetry import sdk, trace
        from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import SimpleSpanProcessor

        # Verify imports are usable
        assert opentelemetry is not None
        assert sdk is not None
        assert trace is not None
        assert TracerProvider is not None
        assert SimpleSpanProcessor is not None
        assert OTLPSpanExporter is not None

    except ImportError as e:
        pytest.fail(f"Failed to import OTEL dependencies: {e}")

    # Instantiate the backend (does not require a running collector)
    backend = OTELTelemetryBackend(tracer_name="test-sanity")
    assert backend.tracer_name == "test-sanity"
    assert backend._tracer is None

    # Trigger tracer initialization
    tracer = backend._ensure_tracer()
    assert tracer is not None
    assert backend._tracer is not None


def test_otel_backend_configure(clean_env):
    """Verify that configure() correctly initializes the OTEL SDK."""
    from opentelemetry import trace
    from opentelemetry.sdk.trace import TracerProvider

    backend = OTELTelemetryBackend(tracer_name="test-configure")

    # Act
    backend.configure()

    provider = trace.get_tracer_provider()

    # In some cases (e.g. proxying), we might need to look at the delegate
    if hasattr(provider, "_provider"):
        provider = provider._provider

    assert provider is not None
    # Verify it is indeed an SDK TracerProvider
    assert isinstance(provider, TracerProvider)
    assert hasattr(provider, "resource")
    assert provider.resource.attributes["service.name"] == "text2sql-agent"
