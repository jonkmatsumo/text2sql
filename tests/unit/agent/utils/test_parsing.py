"""Tests for agent parsing utilities."""

from agent.utils.parsing import parse_tool_output
from common.models.error_metadata import ErrorCategory


def test_parse_tool_output_handles_malformed_json_explicitly():
    """Verify that malformed JSON triggers an error envelope instead of silent failure."""
    malformed_input = '{"key": "value", invalid}'

    results = parse_tool_output(malformed_input)

    assert len(results) == 1
    error_env = results[0]

    assert error_env["schema_version"] == "1.0"
    assert error_env["error"]["category"] == ErrorCategory.TOOL_RESPONSE_MALFORMED.value
    assert "Malformed tool response" in error_env["error"]["message"]


def test_parse_tool_output_preserves_valid_and_signals_invalid():
    """Verify that valid chunks are preserved while invalid ones signal error."""
    mixed_input = ['{"valid": "data"}', '{"invalid": parse_error}']

    results = parse_tool_output(mixed_input)

    assert len(results) == 2
    assert results[0] == {"valid": "data"}
    assert results[1]["error"]["category"] == ErrorCategory.TOOL_RESPONSE_MALFORMED.value
