"""Tests for bounded context utilities."""

from agent.utils.bounded_context import append_bounded


def test_append_bounded_list_item_count():
    """Verify list truncation by item count."""
    context = ["a", "b", "c"]
    result = append_bounded(context, "d", max_items=2)
    assert result == ["c", "d"]


def test_append_bounded_list_char_count():
    """Verify list truncation by character count."""
    context = ["long_string", "b"]
    # Total chars = 11 + 1 = 12. Add "c" (len 1) -> 13.
    # If max_chars is 5, "long_string" must go.
    result = append_bounded(context, "c", max_chars=5)
    assert result == ["b", "c"]


def test_append_bounded_string_char_count():
    """Verify string truncation by character count."""
    context = "initial"
    # len = 7. Add "added" (len 5) -> "initial\nadded" (len 13)
    # If max_chars is 10, should keep last 10 chars.
    result = append_bounded(context, "added", max_chars=10)
    assert len(result) == 10
    assert result == "tial\nadded"


def test_append_bounded_single_item_too_large():
    """Verify truncation of a single item that exceeds max_chars."""
    context = []
    large_item = "a" * 100
    result = append_bounded(context, large_item, max_chars=10)
    assert result == ["a" * 10]
