"""Tests for typed tool envelopes."""

import json

import pytest

from common.models.tool_envelopes import (
    ExecuteSQLQueryMetadata,
    ExecuteSQLQueryResponseEnvelope,
    parse_execute_sql_response,
)


def test_envelope_creation_valid():
    """Test creating a valid envelope."""
    env = ExecuteSQLQueryResponseEnvelope(
        rows=[{"id": 1}], metadata=ExecuteSQLQueryMetadata(rows_returned=1)
    )
    assert env.rows == [{"id": 1}]
    assert env.metadata.rows_returned == 1
    assert not env.metadata.is_truncated
    assert env.metadata.tool_version == "v1"
    assert env.schema_version == "1.0"


def test_envelope_validation_missing_metadata():
    """Test validation fails if metadata is missing."""
    with pytest.raises(ValueError):
        ExecuteSQLQueryResponseEnvelope(rows=[])


def test_parse_json_string():
    """Test parsing a JSON string into an envelope."""
    payload = {"rows": [{"a": 1}], "metadata": {"rows_returned": 1, "is_truncated": False}}
    json_str = json.dumps(payload)
    env = parse_execute_sql_response(json_str)
    assert isinstance(env, ExecuteSQLQueryResponseEnvelope)
    assert env.rows[0]["a"] == 1


def test_parse_dict():
    """Test parsing a dictionary."""
    payload = {"rows": [{"a": 1}], "metadata": {"rows_returned": 1}}
    env = parse_execute_sql_response(payload)
    assert isinstance(env, ExecuteSQLQueryResponseEnvelope)
    assert env.metadata.tool_version == "v1"


def test_parse_error_string():
    """Test parsing a raw error string."""
    env = parse_execute_sql_response("Database error")
    assert env.is_error()
    assert env.error is not None
    assert env.error.message == "Database error"
    assert env.rows == []


def test_parse_error_dict():
    """Test parsing an error dictionary."""
    payload = {"error": "Syntax error", "error_category": "syntax"}
    env = parse_execute_sql_response(payload)
    assert env.is_error()
    assert env.error.message == "Syntax error"
    assert env.error.category == "syntax"


def test_metadata_defaults():
    """Test metadata default values."""
    meta = ExecuteSQLQueryMetadata(rows_returned=5)
    assert meta.tool_version == "v1"
    assert not meta.is_truncated
    assert not meta.is_limited
    assert not meta.cap_detected
