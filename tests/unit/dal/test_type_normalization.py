"""Unit tests for display-only type normalization."""

from dal.type_normalization import normalize_type_for_display


def test_normalize_integer_types() -> None:
    """Normalize integer family types."""
    assert normalize_type_for_display("int4") == "int"
    assert normalize_type_for_display("BIGINT") == "bigint"


def test_normalize_string_types() -> None:
    """Normalize string family types."""
    assert normalize_type_for_display("varchar(255)") == "string"
    assert normalize_type_for_display("character varying") == "string"


def test_normalize_time_and_json_types() -> None:
    """Normalize temporal and JSON types."""
    assert normalize_type_for_display("TIMESTAMP_NTZ") == "timestamp"
    assert normalize_type_for_display("jsonb") == "json"


def test_passthrough_unknown_type() -> None:
    """Preserve unknown types for display."""
    assert normalize_type_for_display("geography") == "geography"
