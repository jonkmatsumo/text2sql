"""Tests for telemetry safety and redaction policies."""

from unittest.mock import MagicMock, patch

from agent.telemetry import OTELTelemetrySpan
from common.sanitization.bounding import redact_recursive


def test_telemetry_methods_swallow_exceptions():
    """Verify that set_attribute does not raise even if internal logic fails."""
    mock_span = MagicMock()
    span = OTELTelemetrySpan(mock_span)

    # Simulate an exception in truncate_json or similar called by set_attribute
    with patch("agent.telemetry_schema.bound_attribute", side_effect=ValueError("Boom")):
        # Should not raise
        span.set_attribute("key", "value")

    with patch("agent.telemetry_schema.truncate_json", side_effect=ValueError("Boom")):
        # Should not raise
        span.set_inputs({"key": "value"})
        span.set_outputs({"key": "value"})


def test_redaction_allowlist_token_usage():
    """Verify token_usage keys are not redacted despite containing 'token'."""
    data = {
        "llm.token_usage.input_tokens": 100,
        "llm.token_usage.output_tokens": 50,
        "api_token": "secret123",
        "nested": {"token_usage": 10, "secret_key": "hidden"},
    }

    redacted = redact_recursive(data)

    assert redacted["llm.token_usage.input_tokens"] == 100
    assert redacted["llm.token_usage.output_tokens"] == 50
    assert redacted["api_token"] == "<redacted>"
    assert redacted["nested"]["token_usage"] == 10
    assert redacted["nested"]["secret_key"] == "<redacted>"


def test_redaction_allowlist_page_token():
    """Verify page_token is allowed."""
    data = {"next_page_token": "abc-123"}
    redacted = redact_recursive(data)
    assert redacted["next_page_token"] == "abc-123"
