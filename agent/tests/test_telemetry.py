import unittest
from unittest.mock import patch

# Add src to path if needed (depending on test runner config)
from agent_core.telemetry import (
    InMemoryTelemetryBackend,
    OTELTelemetryBackend,
    SpanType,
    TelemetryService,
)
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import StatusCode


class TestTelemetryService(unittest.TestCase):
    """Unit tests for the TelemetryService and its backends."""

    def test_in_memory_backend(self):
        """Test the InMemoryTelemetryBackend captures data correctly."""
        backend = InMemoryTelemetryBackend()
        service = TelemetryService(backend=backend)

        with service.start_span(
            name="test_span",
            span_type=SpanType.CHAIN,
            inputs={"input_key": "input_val"},
            attributes={"attr_key": "attr_val"},
        ) as span:
            span.set_outputs({"output_key": "output_val"})
            span.add_event("test_event", {"event_attr": "event_val"})

        self.assertEqual(len(backend.spans), 1)
        span_data = backend.spans[0]
        self.assertEqual(span_data.name, "test_span")
        self.assertEqual(span_data.span_type, SpanType.CHAIN)
        self.assertEqual(span_data.inputs, {"input_key": "input_val"})
        self.assertEqual(span_data.outputs, {"output_key": "output_val"})
        self.assertEqual(span_data.inputs, {"input_key": "input_val"})
        self.assertEqual(span_data.outputs, {"output_key": "output_val"})
        # Verify attributes match (ignoring auto-injected contract attrs)
        attrs = span_data.attributes.copy()
        attrs.pop("event.seq", None)
        attrs.pop("event.type", None)
        attrs.pop("event.name", None)
        self.assertEqual(attrs, {"attr_key": "attr_val"})
        self.assertEqual(len(span_data.events), 1)
        self.assertEqual(span_data.events[0]["name"], "test_event")
        self.assertTrue(span_data.is_finished)

    def test_update_current_trace(self):
        """Test updating trace metadata via the service."""
        backend = InMemoryTelemetryBackend()
        service = TelemetryService(backend=backend)

        metadata = {"user_id": "123", "session_id": "abc"}
        service.update_current_trace(metadata)

        self.assertEqual(backend.trace_metadata, metadata)

    def test_configure(self):
        """Test configuring the backend via the service."""
        backend = InMemoryTelemetryBackend()
        service = TelemetryService(backend=backend)

        # tracking_uri and autolog are legacy params and are intentionally NOT passed to backend
        service.configure(
            tracking_uri="http://test:5000",
            autolog=True,
            run_tracer_inline=True,
            custom_param="custom_value",
        )

        # Verify custom params are passed
        self.assertEqual(backend.config["custom_param"], "custom_value")
        # Verify legacy params are swallowed/ignored as per implementation
        self.assertNotIn("tracking_uri", backend.config)
        self.assertNotIn("autolog", backend.config)

    def test_otel_backend(self):
        """Test the OTELTelemetryBackend produces correct spans and attributes."""
        # Setup in-memory OTEL SDK
        exporter = InMemorySpanExporter()
        provider = TracerProvider()
        provider.add_span_processor(SimpleSpanProcessor(exporter))

        # We need to ensure OTELTelemetryBackend uses this provider
        with patch("opentelemetry.trace.get_tracer") as mock_get_tracer:
            # Re-implementing a bit of the SDK logic to ensure our test uses the provider
            mock_get_tracer.side_effect = lambda name: provider.get_tracer(name)

            backend = OTELTelemetryBackend()
            service = TelemetryService(backend=backend)

            # Test span creation and attributes
            with service.start_span(
                name="otel_test",
                span_type=SpanType.RETRIEVER,
                inputs={"query": "sql"},
                attributes={"custom": "val"},
            ) as span:
                span.set_outputs({"status": "ok"})
                span.add_event("event_done", {"p": 1})

            # Test error status
            with service.start_span(name="otel_error") as span:
                span.set_outputs({"error": "failed_check"})

        spans = exporter.get_finished_spans()
        self.assertEqual(len(spans), 2)

        # Verify first span
        s1 = spans[0]
        self.assertEqual(s1.name, "otel_test")
        self.assertEqual(s1.attributes["span.type"], SpanType.RETRIEVER.value)
        self.assertEqual(s1.attributes["custom"], "val")
        # Inputs are JSON stringified
        import json

        inputs = json.loads(s1.attributes["telemetry.inputs_json"])
        self.assertEqual(inputs, {"query": "sql"})
        outputs = json.loads(s1.attributes["telemetry.outputs_json"])
        self.assertEqual(outputs, {"status": "ok"})
        self.assertEqual(len(s1.events), 1)
        self.assertEqual(s1.events[0].name, "event_done")

        # Verify second span (Error)
        s2 = spans[1]
        self.assertEqual(s2.name, "otel_error")
        self.assertEqual(s2.status.status_code, StatusCode.ERROR)
        self.assertEqual(s2.status.description, "failed_check")
        # Error is now JSON structured
        error_json = json.loads(s2.attributes["telemetry.error_json"])
        self.assertEqual(error_json["error"], "failed_check")

    def test_context_propagation(self):
        """Test that context can be captured and restored in OTEL."""
        provider = TracerProvider()
        tracer = provider.get_tracer("test")
        exporter = InMemorySpanExporter()
        provider.add_span_processor(SimpleSpanProcessor(exporter))

        backend = OTELTelemetryBackend()
        service = TelemetryService(backend=backend)

        # 1. Start a parent span and capture context
        with patch("opentelemetry.trace.get_tracer", return_value=tracer):
            with service.start_span("root"):
                ctx = service.capture_context()
                self.assertIsNotNone(ctx.otel_context)

            # 2. Start a child span using the captured context
            with service.use_context(ctx):
                with service.start_span("child"):
                    pass

        spans = exporter.get_finished_spans()
        self.assertEqual(len(spans), 2)

        # spans are returned in finish order: child, then root
        root = next(s for s in spans if s.name == "root")
        child = next(s for s in spans if s.name == "child")

        self.assertEqual(child.parent.span_id, root.context.span_id)
        self.assertEqual(child.context.trace_id, root.context.trace_id)

    def test_backend_selection_otel_default(self):
        """Test that the service defaults to OTEL (or memory if no config) but never Dual/MLflow."""
        # By default in tests if TELEMETRY_BACKEND is unset, it might be InMemory or OTEL
        # depending on init logic. Assuming default init with no args uses env vars.

        # Case 1: Explicit OTEL
        with patch.dict("os.environ", {"TELEMETRY_BACKEND": "otel"}):
            service = TelemetryService()
            self.assertIsInstance(service._backend, OTELTelemetryBackend)

    def test_backend_selection_legacy_fallback(self):
        """Test that legacy configuration values now fallback to OTEL or compatible default."""
        # If user still has TELEMETRY_BACKEND=dual or mlflow, it should NOT crash,
        # but likely use OTEL or just log a warning and use default.
        # This depends on strict implementation of TelemetryService.__init__.
        # If the code enforces "no Dual", then this test ensures we don't accidentally get it.

        # We assume the implementation treats unknown/deprecated backends as OTEL or logs warning.
        # Let's verify it DOES NOT return DualTelemetryBackend.

        # NOTE: checking for DualTelemetryBackend existence is hard if we deleted the import,
        # so we just check it is OTELTelemetryBackend or InMemory.

        with patch.dict("os.environ", {"TELEMETRY_BACKEND": "mlflow"}):
            # If the code was updated to map 'mlflow' -> OTEL or just ignore it
            service = TelemetryService()
            self.assertIsInstance(service._backend, OTELTelemetryBackend)

        with patch.dict("os.environ", {"TELEMETRY_BACKEND": "dual"}):
            service = TelemetryService()
            # Should be OTEL now
            self.assertIsInstance(service._backend, OTELTelemetryBackend)


if __name__ == "__main__":
    unittest.main()
