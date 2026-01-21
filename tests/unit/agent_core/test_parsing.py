"""Unit tests for MCP tool output parsing.

Tests for:
- detect_double_json_encoding()
- normalize_payload()
- parse_tool_output()

Covers both SDK payloads (single encoding) and wrapped payloads (double encoding).
"""

import json

import pytest

from agent_core.utils.parsing import (
    detect_double_json_encoding,
    normalize_payload,
    parse_tool_output,
)

# ==============================================================================
# Fixtures
# ==============================================================================


@pytest.fixture
def sdk_success_payload():
    """SDK-style payload: single-encoded JSON in TextContent.text."""
    return json.dumps({"tables": ["film", "actor", "customer"]})


@pytest.fixture
def sdk_error_payload():
    """SDK-style error payload: error message in TextContent."""
    return json.dumps({"error": "Query execution failed", "code": "SYNTAX_ERROR"})


@pytest.fixture
def double_encoded_success():
    """Return a double-encoded JSON success string."""
    inner = json.dumps({"tables": ["film", "actor", "customer"]})
    return json.dumps(inner)  # Double-encoded


@pytest.fixture
def double_encoded_error():
    """Return a double-encoded JSON error message."""
    inner = json.dumps({"error": "Connection failed"})
    return json.dumps(inner)


# ==============================================================================
# Tests: detect_double_json_encoding
# ==============================================================================


class TestDetectDoubleJsonEncoding:
    """Tests for detect_double_json_encoding()."""

    def test_single_encoded_dict_returns_false(self):
        """Single-encoded dict JSON should return False."""
        payload = '{"status": "success", "data": [1, 2, 3]}'
        assert detect_double_json_encoding(payload) is False

    def test_double_encoded_returns_true(self):
        """Double-encoded JSON should return True."""
        inner = '{"status": "success", "data": [1, 2, 3]}'
        payload = json.dumps(inner)
        assert detect_double_json_encoding(payload) is True

    def test_non_string_returns_false(self):
        """Non-string input should return False."""
        assert detect_double_json_encoding({"key": "value"}) is False
        assert detect_double_json_encoding([1, 2, 3]) is False
        assert detect_double_json_encoding(None) is False

    def test_plain_text_returns_false(self):
        """Plain text (not JSON) should return False."""
        assert detect_double_json_encoding("Hello World") is False

    def test_json_string_value_returns_true(self):
        """A JSON string that contains another JSON string."""
        inner = json.dumps({"x": 1})
        outer = json.dumps(inner)
        assert detect_double_json_encoding(outer) is True


# ==============================================================================
# Tests: normalize_payload
# ==============================================================================


class TestNormalizePayload:
    """Tests for normalize_payload()."""

    def test_dict_passthrough(self):
        """Dict should pass through unchanged."""
        data = {"tables": ["film", "actor"]}
        assert normalize_payload(data) == data

    def test_list_passthrough(self):
        """List should pass through unchanged."""
        data = [{"name": "film"}, {"name": "actor"}]
        assert normalize_payload(data) == data

    def test_single_encoded_json(self, sdk_success_payload):
        """Single-encoded JSON string should be parsed once."""
        result = normalize_payload(sdk_success_payload)
        assert result == {"tables": ["film", "actor", "customer"]}

    def test_double_encoded_json(self, double_encoded_success):
        """Double-encoded JSON should be parsed twice."""
        result = normalize_payload(double_encoded_success)
        assert result == {"tables": ["film", "actor", "customer"]}

    def test_plain_text_returns_as_is(self):
        """Plain text that's not JSON should return unchanged."""
        result = normalize_payload("This is an error message")
        assert result == "This is an error message"

    def test_empty_string(self):
        """Empty string should return as-is."""
        result = normalize_payload("")
        assert result == ""

    def test_text_content_object(self):
        """Object with .text attribute should extract text."""

        class MockTextContent:
            text = '{"key": "value"}'

        result = normalize_payload(MockTextContent())
        assert result == {"key": "value"}

    def test_number_passthrough(self):
        """Numbers should pass through unchanged."""
        assert normalize_payload(42) == 42
        assert normalize_payload(3.14) == 3.14


# ==============================================================================
# Tests: parse_tool_output
# ==============================================================================


class TestParseToolOutput:
    """Tests for parse_tool_output()."""

    def test_sdk_text_content_list(self, sdk_success_payload):
        """Parse SDK-style [{"type": "text", "text": ...}] format."""
        tool_output = [{"type": "text", "text": sdk_success_payload}]
        result = parse_tool_output(tool_output)
        assert len(result) == 1
        assert result[0] == {"tables": ["film", "actor", "customer"]}

    def test_double_encoded_result(self, double_encoded_success):
        """Parse double-encoded format."""
        tool_output = [{"type": "text", "text": double_encoded_success}]
        result = parse_tool_output(tool_output)
        assert len(result) == 1
        assert result[0] == {"tables": ["film", "actor", "customer"]}

    def test_raw_json_string(self, sdk_success_payload):
        """Parse raw JSON string input."""
        result = parse_tool_output(sdk_success_payload)
        assert len(result) == 1
        assert result[0] == {"tables": ["film", "actor", "customer"]}

    def test_list_result_flattened(self):
        """List results should be flattened into aggregated results."""
        data = [{"name": "film"}, {"name": "actor"}]
        tool_output = json.dumps(data)
        result = parse_tool_output(tool_output)
        assert len(result) == 2
        assert result[0] == {"name": "film"}
        assert result[1] == {"name": "actor"}

    def test_tuple_with_artifact(self):
        """Handle (content, artifact) tuple format."""
        data = {"tables": ["film"]}
        payload = json.dumps(data)
        tool_output = ([{"type": "text", "text": payload}], "artifact_metadata")
        result = parse_tool_output(tool_output)
        assert len(result) == 1
        assert result[0] == {"tables": ["film"]}

    def test_dict_without_type_key(self):
        """Dict without 'type' key should be preserved."""
        data = {"tables": ["film"], "count": 1}
        result = parse_tool_output([data])
        assert data in result

    def test_object_with_text_attribute(self):
        """Object with .text attribute should be handled."""

        class MockContent:
            text = '{"key": "value"}'

        result = parse_tool_output([MockContent()])
        assert len(result) == 1
        assert result[0] == {"key": "value"}

    def test_empty_input(self):
        """Empty list should return empty list."""
        assert parse_tool_output([]) == []

    def test_mixed_content_types(self):
        """Handle mixed content types in same output."""
        tool_output = [
            {"type": "text", "text": json.dumps({"table": "film"})},
            {"type": "text", "text": json.dumps({"table": "actor"})},
        ]
        result = parse_tool_output(tool_output)
        assert len(result) == 2
        assert result[0] == {"table": "film"}
        assert result[1] == {"table": "actor"}
