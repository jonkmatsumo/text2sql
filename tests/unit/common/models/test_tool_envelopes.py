"""Tests for tool envelope models."""

from common.models.tool_envelopes import (
    ExecuteSQLQueryMetadata,
    GenericToolMetadata,
    GenericToolResponseEnvelope,
)


def test_generic_tool_metadata_snapshot_id():
    """Verify snapshot_id is preserved in GenericToolMetadata."""
    meta = GenericToolMetadata(snapshot_id="snap-123", provider="postgres")
    assert meta.snapshot_id == "snap-123"

    # Verify serialization
    dump = meta.model_dump()
    assert dump["snapshot_id"] == "snap-123"


def test_generic_tool_metadata_items_returned_alias():
    """Verify items_returned and returned_count aliases in GenericToolMetadata."""
    # Test items_returned -> returned_count
    meta1 = GenericToolMetadata(items_returned=5)
    assert meta1.items_returned == 5
    assert meta1.returned_count == 5

    # Test returned_count -> items_returned
    meta2 = GenericToolMetadata(returned_count=10)
    assert meta2.items_returned == 10
    assert meta2.returned_count == 10


def test_generic_tool_response_envelope_serialization():
    """Verify GenericToolResponseEnvelope round-trips correctly with metadata."""
    envelope = GenericToolResponseEnvelope(
        result={"key": "value"},
        metadata=GenericToolMetadata(snapshot_id="snap-456", items_returned=1),
    )

    json_str = envelope.model_dump_json()
    assert '"snapshot_id":"snap-456"' in json_str
    assert '"items_returned":1' in json_str
    assert '"returned_count":1' in json_str


def test_execute_sql_query_metadata_truncation_reason_alias():
    """Verify truncation_reason and partial_reason aliases in ExecuteSQLQueryMetadata."""
    # Test partial_reason -> truncation_reason
    meta1 = ExecuteSQLQueryMetadata(rows_returned=0, partial_reason="MAX_ROWS")
    assert meta1.partial_reason == "MAX_ROWS"
    assert meta1.truncation_reason == "MAX_ROWS"

    # Test truncation_reason -> partial_reason
    meta2 = ExecuteSQLQueryMetadata(rows_returned=0, truncation_reason="SIZE_LIMIT")
    assert meta2.partial_reason == "SIZE_LIMIT"
    assert meta2.truncation_reason == "SIZE_LIMIT"
