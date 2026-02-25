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


def test_execute_sql_query_metadata_partial_aliases_is_truncated():
    """Verify partial/is_truncated/truncated aliases remain synchronized."""
    meta1 = ExecuteSQLQueryMetadata(rows_returned=1, partial=True)
    assert meta1.partial is True
    assert meta1.is_truncated is True
    assert meta1.truncated is True

    meta2 = ExecuteSQLQueryMetadata(rows_returned=1, is_truncated=False)
    assert meta2.partial is False
    assert meta2.truncated is False


def test_execute_sql_query_metadata_items_returned_aliases():
    """Verify items_returned stays aligned with rows_returned/returned_count."""
    meta1 = ExecuteSQLQueryMetadata(rows_returned=3, items_returned=3)
    assert meta1.rows_returned == 3
    assert meta1.items_returned == 3
    assert meta1.returned_count == 3

    meta2 = ExecuteSQLQueryMetadata(returned_count=4)
    assert meta2.rows_returned == 4
    assert meta2.items_returned == 4


def test_execute_sql_query_metadata_keyset_cursor_aliasing_is_one_way():
    """next_keyset_cursor should populate next_page_token, but not vice versa."""
    keyset_meta = ExecuteSQLQueryMetadata(
        rows_returned=1,
        pagination_mode_used="keyset",
        next_keyset_cursor="ks-1",
    )
    assert keyset_meta.next_keyset_cursor == "ks-1"
    assert keyset_meta.next_page_token == "ks-1"

    page_token_meta = ExecuteSQLQueryMetadata(
        rows_returned=1,
        pagination_mode_used="keyset",
        next_page_token="offset-1",
    )
    assert page_token_meta.next_page_token == "offset-1"
    assert page_token_meta.next_keyset_cursor is None
