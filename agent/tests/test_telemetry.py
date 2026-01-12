import unittest
from unittest.mock import MagicMock, patch

# Add src to path if needed (depending on test runner config)
from agent_core.telemetry import (
    DualTelemetryBackend,
    InMemoryTelemetryBackend,
    MlflowTelemetryBackend,
    OTELTelemetryBackend,
    SpanType,
    TelemetryBackend,
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
        self.assertEqual(span_data.attributes, {"attr_key": "attr_val"})
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

        service.configure(tracking_uri="http://test:5000", autolog=True, run_tracer_inline=True)

        self.assertEqual(backend.config["tracking_uri"], "http://test:5000")
        self.assertTrue(backend.config["autolog"])
        self.assertTrue(backend.config["run_tracer_inline"])

    @patch("mlflow.start_span")
    @patch("mlflow.set_tracking_uri")
    @patch("mlflow.update_current_trace")
    def test_mlflow_backend(self, mock_update, mock_set_uri, mock_start_span):
        """Test the MlflowTelemetryBackend delegates to mlflow correctly."""
        # Mock MLflow span context manager
        mock_ml_span = MagicMock()
        mock_start_span.return_value.__enter__.return_value = mock_ml_span

        # Mock mlflow.langchain
        mock_langchain = MagicMock()
        mock_autolog = mock_langchain.autolog

        with patch.dict("sys.modules", {"mlflow.langchain": mock_langchain}):
            backend = MlflowTelemetryBackend()
            service = TelemetryService(backend=backend)

            # Test configure
            service.configure(
                tracking_uri="http://mlflow:5000", autolog=True, run_tracer_inline=True
            )
            mock_set_uri.assert_called_once_with("http://mlflow:5000")
            mock_autolog.assert_called_once_with(run_tracer_inline=True)

            # Test start_span
            with service.start_span(
                name="ml_span",
                span_type=SpanType.TOOL,
                inputs={"in": "1"},
            ) as span:
                span.set_outputs({"out": "2"})
                span.set_attribute("attr", "3")
                span.add_event("event", {"e": "4"})

            mock_start_span.assert_called_once()
            call_args = mock_start_span.call_args
            self.assertEqual(call_args.kwargs["name"], "ml_span")
            # We don't strictly check span_type here as it involves mapping lookup logic
            # that we trust, but we could if needed.
            mock_ml_span.set_inputs.assert_called_with({"in": "1"})
            mock_ml_span.set_outputs.assert_called_with({"out": "2"})
            mock_ml_span.set_attribute.assert_any_call("attr", "3")
            mock_ml_span.add_event.assert_called_with("event", {"e": "4"})

            service.update_current_trace({"m": "n"})
            mock_update.assert_called_once_with(metadata={"m": "n"})

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

        self.assertEqual(json.loads(s1.attributes["telemetry.inputs_json"]), {"query": "sql"})
        self.assertEqual(json.loads(s1.attributes["telemetry.outputs_json"]), {"status": "ok"})
        self.assertEqual(len(s1.events), 1)
        self.assertEqual(s1.events[0].name, "event_done")

        # Verify second span (Error)
        s2 = spans[1]
        self.assertEqual(s2.name, "otel_error")
        self.assertEqual(s2.status.status_code, StatusCode.ERROR)
        self.assertEqual(s2.status.description, "failed_check")
        self.assertEqual(s2.attributes["telemetry.error"], "failed_check")

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

    def test_backend_selection(self):
        """Test that the service selects the correct backend based on env var."""
        with patch.dict("os.environ", {"TELEMETRY_BACKEND": "otel"}):
            service = TelemetryService()
            self.assertIsInstance(service._backend, OTELTelemetryBackend)

        with patch.dict("os.environ", {"TELEMETRY_BACKEND": "dual"}):
            service = TelemetryService()
            self.assertIsInstance(service._backend, DualTelemetryBackend)

    def test_dual_write_success(self):
        """Test that DualTelemetryBackend writes to both backends."""
        p = InMemoryTelemetryBackend()
        s = InMemoryTelemetryBackend()
        backend = DualTelemetryBackend(p, s)
        service = TelemetryService(backend=backend)

        with service.start_span("dual_test") as span:
            span.set_attribute("k", "v")

        self.assertEqual(len(p.spans), 1)
        self.assertEqual(len(s.spans), 1)
        self.assertEqual(p.spans[0].attributes["k"], "v")
        self.assertEqual(s.spans[0].attributes["k"], "v")

    def test_dual_write_secondary_failure_isolation(self):
        """Test that DualTelemetryBackend survives secondary backend failure."""
        p = InMemoryTelemetryBackend()
        s = MagicMock(spec=TelemetryBackend)
        # Mock start_span to return a context manager that fails or just fail directly
        s.start_span.side_effect = Exception("Secondary Crash")

        backend = DualTelemetryBackend(p, s)
        service = TelemetryService(backend=backend)

        # This should NOT raise
        with service.start_span("isolate_test") as span:
            span.set_attribute("k", "v")

        self.assertEqual(len(p.spans), 1)
        self.assertEqual(p.spans[0].attributes["k"], "v")
        self.assertTrue(p.spans[0].is_finished)


if __name__ == "__main__":
    unittest.main()
