"""Unit tests for execution-boundary resource containment helpers."""

import json

from dal.resource_containment import enforce_byte_limit, enforce_row_limit


def _json_size(payload) -> int:
    return len(json.dumps(payload, default=str, separators=(",", ":")).encode("utf-8"))


def test_enforce_row_limit_truncates_when_rows_exceed_cap():
    """Row cap should truncate oversized result sets at the boundary."""
    rows = [{"id": idx} for idx in range(6)]

    bounded = enforce_row_limit(rows, max_rows=3, enforce=True)

    assert bounded.partial is True
    assert bounded.partial_reason == "max_rows"
    assert bounded.items_returned == 3
    assert bounded.rows == [{"id": 0}, {"id": 1}, {"id": 2}]


def test_enforce_row_limit_does_not_truncate_when_rows_within_cap():
    """Rows at or below cap should pass through without truncation."""
    rows = [{"id": idx} for idx in range(3)]

    bounded = enforce_row_limit(rows, max_rows=3, enforce=True)

    assert bounded.partial is False
    assert bounded.partial_reason is None
    assert bounded.items_returned == 3
    assert bounded.rows == rows


def test_enforce_row_limit_preserves_deterministic_order():
    """Capping should keep original row ordering deterministic."""
    rows = [{"id": 9}, {"id": 2}, {"id": 7}, {"id": 4}]

    bounded = enforce_row_limit(rows, max_rows=2, enforce=True)

    assert bounded.rows == [{"id": 9}, {"id": 2}]


def test_enforce_byte_limit_truncates_oversized_rows():
    """Byte cap should truncate when the first row exceeds the configured budget."""
    rows = [{"blob": "x" * 64}]
    row_size = _json_size(rows[0])

    bounded = enforce_byte_limit(rows, max_bytes=row_size - 1, enforce=True)

    assert bounded.partial is True
    assert bounded.partial_reason == "max_bytes"
    assert bounded.items_returned == 0
    assert bounded.rows == []


def test_enforce_byte_limit_handles_mixed_row_sizes_deterministically():
    """Byte cap should retain rows in order up to the last fully fitting row."""
    rows = [{"id": 1}, {"blob": "x" * 24}, {"id": 3}]
    cap = _json_size({}) + _json_size(rows[0]) + _json_size(rows[1])

    bounded = enforce_byte_limit(rows, max_bytes=cap, enforce=True)

    assert bounded.partial is True
    assert bounded.rows == [{"id": 1}, {"blob": "x" * 24}]
    assert bounded.items_returned == 2


def test_enforce_byte_limit_tracks_bytes_with_envelope_overhead():
    """Byte accounting should include envelope overhead when provided."""
    rows = [{"id": 1}]
    overhead = {"metadata": {}, "rows": []}

    bounded = enforce_byte_limit(rows, max_bytes=10_000, enforce=True, envelope_overhead=overhead)

    assert bounded.partial is False
    assert bounded.bytes_returned >= _json_size(overhead) + _json_size(rows[0])
