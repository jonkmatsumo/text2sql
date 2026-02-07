"""Tests for execute node typed envelope parsing."""

import json
from unittest.mock import MagicMock, patch

from agent.nodes.execute import _parse_tool_response_with_shim
from common.models.tool_envelopes import ExecuteSQLQueryResponseEnvelope


def test_parse_valid_envelope():
    """Test parsing a valid envelope dictionary."""
    payload = {
        "schema_version": "1.0",
        "rows": [{"id": 1}],
        "metadata": {"rows_returned": 1, "is_truncated": False},
    }
    env = _parse_tool_response_with_shim(payload)
    assert isinstance(env, ExecuteSQLQueryResponseEnvelope)
    assert env.rows[0]["id"] == 1
    assert not env.is_error()


def test_parse_error_envelope():
    """Test parsing an error envelope."""
    payload = {
        "rows": [],
        "metadata": {"rows_returned": 0, "is_truncated": False},
        "error": {
            "message": "DB Error",
            "category": "system",
            "is_retryable": False,
            "provider": "test",
        },
    }
    env = _parse_tool_response_with_shim(payload)
    assert env.is_error()
    assert env.error.message == "DB Error"


def test_legacy_shim_disabled_by_default():
    """Test legacy list payload fails when shim disabled."""
    payload = [{"id": 1}, {"id": 2}]
    # Default behavior: shim disabled
    env = _parse_tool_response_with_shim(payload)
    assert env.is_error()
    assert "Invalid payload type" in env.error_message


def test_legacy_shim_enabled(monkeypatch):
    """Test legacy list payload wraps when shim enabled."""
    monkeypatch.setenv("AGENT_ENABLE_LEGACY_TOOL_SHIM", "true")
    payload = [{"id": 1}, {"id": 2}]

    with patch("agent.nodes.execute.telemetry") as mock_telemetry:
        mock_span = MagicMock()
        mock_telemetry.get_current_span.return_value = mock_span

        env = _parse_tool_response_with_shim(payload)

        assert isinstance(env, ExecuteSQLQueryResponseEnvelope)
        assert len(env.rows) == 2
        assert env.rows[0]["id"] == 1
        assert not env.is_error()

        # Verify telemetry event
        mock_span.add_event.assert_called_with("tool.response_legacy_shape", {"type": "list"})


def test_parse_json_string_envelope():
    """Test parsing a JSON string envelope."""
    payload = {"rows": [{"id": 1}], "metadata": {"rows_returned": 1, "is_truncated": False}}
    env = _parse_tool_response_with_shim(json.dumps(payload))
    assert env.rows[0]["id"] == 1


def test_legacy_shim_json_string(monkeypatch):
    """Test legacy JSON string list payload."""
    monkeypatch.setenv("AGENT_ENABLE_LEGACY_TOOL_SHIM", "true")
    payload = json.dumps([{"id": 1}])

    with patch("agent.nodes.execute.telemetry"):
        env = _parse_tool_response_with_shim(payload)
        assert len(env.rows) == 1
        assert env.rows[0]["id"] == 1
