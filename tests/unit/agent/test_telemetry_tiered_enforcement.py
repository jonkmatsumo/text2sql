"""Tests for telemetry contract enforcement modes."""

from unittest.mock import MagicMock

import pytest

from agent.telemetry import OTELTelemetryBackend, SpanType


def test_contract_violations_warn_by_default(monkeypatch, caplog):
    """Default warn mode should emit warning without raising."""
    monkeypatch.setenv("AGENT_TELEMETRY_CONTRACT_ENFORCE", "warn")
    monkeypatch.setattr("agent.telemetry._CONTRACT_ENFORCE_MODE", None)

    backend = OTELTelemetryBackend()
    mock_tracer = MagicMock()
    backend._tracer = mock_tracer

    with caplog.at_level("WARNING"):
        with backend.start_span("execute_sql", span_type=SpanType.TOOL):
            pass

    assert "Span contract violation for 'execute_sql': missing" in caplog.text


def test_contract_violations_raise_in_error_mode(monkeypatch):
    """Error mode should raise for missing required contract attributes."""
    monkeypatch.setenv("AGENT_TELEMETRY_CONTRACT_ENFORCE", "error")
    monkeypatch.setattr("agent.telemetry._CONTRACT_ENFORCE_MODE", None)

    backend = OTELTelemetryBackend()
    mock_tracer = MagicMock()
    backend._tracer = mock_tracer

    with pytest.raises(ValueError, match="Span contract violation for 'execute_sql': missing"):
        with backend.start_span("execute_sql", span_type=SpanType.TOOL):
            pass
