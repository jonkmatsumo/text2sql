"""Tests for execute node typed envelope parsing."""

import json

from common.models.tool_envelopes import ExecuteSQLQueryResponseEnvelope, parse_execute_sql_response


def test_parse_valid_envelope():
    """Test parsing a valid envelope dictionary."""
    payload = {
        "schema_version": "1.0",
        "rows": [{"id": 1}],
        "metadata": {"rows_returned": 1, "is_truncated": False},
    }
    env = parse_execute_sql_response(payload)
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
    env = parse_execute_sql_response(payload)
    assert env.is_error()
    assert env.error.message == "DB Error"


def test_bare_list_payload_fails():
    """Test legacy list payload fails (shim retired)."""
    payload = [{"id": 1}, {"id": 2}]
    env = parse_execute_sql_response(payload)
    assert env.is_error()
    assert "Invalid payload type" in env.error_message


def test_parse_json_string_envelope():
    """Test parsing a JSON string envelope."""
    payload = {"rows": [{"id": 1}], "metadata": {"rows_returned": 1, "is_truncated": False}}
    env = parse_execute_sql_response(json.dumps(payload))
    assert env.rows[0]["id"] == 1
