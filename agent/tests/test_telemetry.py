import unittest
from unittest.mock import MagicMock, patch

# Add src to path if needed (depending on test runner config)
from agent_core.telemetry import (
    InMemoryTelemetryBackend,
    MlflowTelemetryBackend,
    SpanType,
    TelemetryService,
)


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
            mock_ml_span.set_inputs.assert_called_with({"in": "1"})
            mock_ml_span.set_outputs.assert_called_with({"out": "2"})
            mock_ml_span.set_attribute.assert_any_call("attr", "3")
            mock_ml_span.add_event.assert_called_with("event", {"e": "4"})

            # Test update_current_trace
            service.update_current_trace({"m": "n"})
            mock_update.assert_called_once_with(metadata={"m": "n"})


if __name__ == "__main__":
    unittest.main()
