"""Tests for the telemetry enforcement safety kill switch."""

from unittest.mock import MagicMock, patch

import pytest

from agent.telemetry import InMemoryTelemetryBackend, TelemetryService


@pytest.mark.parametrize("required", [True, False])
def test_telemetry_required_kill_switch_on_attribute_failure(monkeypatch, required):
    """Verify that AGENT_TELEMETRY_REQUIRED re-raises on attribute errors."""
    monkeypatch.setenv("AGENT_TELEMETRY_REQUIRED", "true" if required else "false")

    # We use a backend that we can mock to fail
    backend = InMemoryTelemetryBackend()
    service = TelemetryService(backend=backend)

    # Mock redact_recursive to fail only when we want it to
    with patch("agent.telemetry.redact_recursive", side_effect=RuntimeError("Redaction failed")):
        if required:
            with pytest.raises(RuntimeError, match="Redaction failed"):
                with service.start_span("test_span") as span:
                    span.set_attribute("key", "value")
        else:
            # Should NOT raise, just log
            with service.start_span("test_span") as span:
                span.set_attribute("key", "value")


@pytest.mark.asyncio
async def test_telemetry_required_kill_switch_on_non_recording_span(monkeypatch):
    """Verify that AGENT_TELEMETRY_REQUIRED raises if span is not recording."""
    monkeypatch.setenv("AGENT_TELEMETRY_REQUIRED", "true")
    monkeypatch.setenv("TELEMETRY_BACKEND", "otel")

    from agent.telemetry import TelemetryService

    service = TelemetryService()

    # Mock the OTEL span to be NOT recording
    mock_otel_span = MagicMock()
    mock_otel_span.is_recording.return_value = False

    # Mock tracer.start_as_current_span
    with patch("opentelemetry.trace.get_tracer") as mock_get_tracer:
        mock_tracer = MagicMock()
        mock_tracer.start_as_current_span.return_value.__enter__.return_value = mock_otel_span
        mock_get_tracer.return_value = mock_tracer

        with pytest.raises(RuntimeError, match="is not recording"):
            with service.start_span("critical_operation") as _:
                pass
