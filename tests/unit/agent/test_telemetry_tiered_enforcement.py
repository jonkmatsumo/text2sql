"""Tests for tiered telemetry contract enforcement."""

from unittest.mock import MagicMock

import pytest

from agent.telemetry import OTELTelemetryBackend, SpanType


def test_critical_span_fails_on_missing_attributes():
    """Verify that critical spans raise ValueError if required attributes are missing."""
    backend = OTELTelemetryBackend()
    # Mocking ensure_tracer to return a mock tracer
    mock_tracer = MagicMock()
    backend._tracer = mock_tracer

    # "execute_sql" is a critical span.
    # Its contract requires {"result.is_truncated", "result.rows_returned"}

    with pytest.raises(ValueError, match="Span contract violation for 'execute_sql': missing"):
        with backend.start_span("execute_sql", span_type=SpanType.TOOL):
            # Setting only one of the required attributes
            telemetry_span = backend.get_current_span()
            telemetry_span.set_attribute("result.rows_returned", 10)
            # result.is_truncated is missing!


def test_non_critical_span_warns_on_missing_attributes(caplog):
    """Verify that non-critical spans only warn if required attributes are missing."""
    backend = OTELTelemetryBackend()
    mock_tracer = MagicMock()
    backend._tracer = mock_tracer

    # "cache_lookup" is NOT a critical span.
    # Its contract requires {"cache.hit"}

    with backend.start_span("cache_lookup", span_type=SpanType.TOOL):
        # cache.hit is missing!
        pass

    assert "Span contract violation for 'cache_lookup': missing ['cache.hit']" in caplog.text
