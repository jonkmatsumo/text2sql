"""Tests for tool envelope models."""

from common.models.tool_envelopes import GenericToolMetadata, GenericToolResponseEnvelope


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
