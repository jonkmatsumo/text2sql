import os
from unittest.mock import patch

import pytest
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from agent.telemetry import OTELTelemetryBackend, TelemetryService

pytestmark = pytest.mark.skipif(
    os.getenv("SKIP_OTEL_WORKER_TESTS") == "1",
    reason="OTEL worker tests disabled via SKIP_OTEL_WORKER_TESTS=1",
)


@pytest.fixture(autouse=True)
def reset_sticky_metadata():
    """Reset sticky metadata before each test."""
    import agent.telemetry as telemetry_mod

    token = telemetry_mod._sticky_metadata.set({})
    yield
    telemetry_mod._sticky_metadata.reset(token)


@pytest.fixture
def otel_test_setup():
    """Set up in-memory OTEL SDK for testing."""
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))

    # Reset internal flags if any
    import agent.telemetry as telemetry_mod

    telemetry_mod._otel_initialized = True  # Prevent setup_otel_sdk from re-initializing global

    with patch("opentelemetry.trace.get_tracer") as mock_get_tracer:
        mock_get_tracer.side_effect = lambda name: provider.get_tracer(name)

        backend = OTELTelemetryBackend(tracer_name="test-metadata")
        service = TelemetryService(backend=backend)
        yield service, exporter


def test_metadata_is_sticky(otel_test_setup):
    """Verify that metadata set via update_current_trace is inherited by sub-spans."""
    service, exporter = otel_test_setup

    with service.start_span("parent"):
        # 1. Update trace with metadata
        metadata = {"tenant_id": "123", "environment": "test", "interaction_id": "int-456"}
        service.update_current_trace(metadata)

        # 2. Start a child span
        with service.start_span("child") as child:
            child.set_attribute("child_attr", "foo")

    spans = exporter.get_finished_spans()
    # Finish order: child, then parent
    assert len(spans) == 2
    child_span = next(s for s in spans if s.name == "child")
    parent_span = next(s for s in spans if s.name == "parent")

    # Parent should have the metadata (best effort update)
    assert parent_span.attributes["tenant_id"] == "123"
    assert parent_span.attributes["interaction_id"] == "int-456"

    # Child MUST have the metadata inherited via sticky logic
    assert child_span.attributes["tenant_id"] == "123"
    assert child_span.attributes["environment"] == "test"
    assert child_span.attributes["interaction_id"] == "int-456"
    assert child_span.attributes["child_attr"] == "foo"


def test_metadata_propagation_with_context(otel_test_setup):
    """Verify metadata propagates through capture_context and use_context."""
    service, exporter = otel_test_setup

    # 1. Start root, set metadata, capture context
    with service.start_span("root"):
        service.update_current_trace({"telemetry.session_id": "sess-ABC"})
        ctx = service.capture_context()

    # 2. Use context in another "execution flow" (simulated)
    with service.use_context(ctx):
        with service.start_span("resumed_child"):
            pass

    spans = exporter.get_finished_spans()
    resumed_span = next(s for s in spans if s.name == "resumed_child")

    # Resumed span should have the sticky metadata from the captured context
    assert resumed_span.attributes["telemetry.session_id"] == "sess-ABC"


def test_metadata_reset_after_context_exit(otel_test_setup):
    """Verify sticky metadata is reset when use_context block exits."""
    service, exporter = otel_test_setup

    # 1. Capture context with metadata
    with service.start_span("root"):
        service.update_current_trace({"sticky_key": "val1"})
        ctx = service.capture_context()

    # 2. Use context, then exit
    with service.use_context(ctx):
        pass

    # 3. Create a new span OUTSIDE the context
    with service.start_span("independent"):
        pass

    spans = exporter.get_finished_spans()
    independent_span = next(s for s in spans if s.name == "independent")

    # Should NOT have the sticky metadata from the previous context
    assert "sticky_key" not in independent_span.attributes


def test_metadata_incremental_updates(otel_test_setup):
    """Verify that multiple updates accumulate correctly."""
    service, exporter = otel_test_setup

    with service.start_span("root"):
        service.update_current_trace({"a": 1})
        with service.start_span("child1"):
            service.update_current_trace({"b": 2})
            with service.start_span("child2"):
                pass

    spans = exporter.get_finished_spans()
    child2 = next(s for s in spans if s.name == "child2")

    assert child2.attributes["a"] == 1
    assert child2.attributes["b"] == 2
