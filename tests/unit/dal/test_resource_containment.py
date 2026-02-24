"""Unit tests for execution-boundary resource containment helpers."""

from dal.resource_containment import enforce_row_limit


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
