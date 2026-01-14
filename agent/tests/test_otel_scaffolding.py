import pytest
from agent_core.telemetry import OTELTelemetryBackend


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


def test_otel_backend_configure():
    """Verify that configure() correctly initializes the OTEL SDK."""
    from opentelemetry import trace

    backend = OTELTelemetryBackend(tracer_name="test-configure")

    # Before configure, provider might be the default NoOp provider
    # but we can check if our global _otel_initialized is set after configure
    backend.configure()

    provider = trace.get_tracer_provider()
    assert provider is not None
    # If initialized, it should be a TracerProvider from the SDK
    assert hasattr(provider, "resource")
    assert provider.resource.attributes["service.name"] == "text2sql-agent"
