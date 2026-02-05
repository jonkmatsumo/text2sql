"""Tests for QueryResult pagination fields."""

from dal.query_result import QueryResult


def test_query_result_defaults():
    """Verify QueryResult defaults pagination fields to None."""
    result = QueryResult(rows=[])

    assert result.next_page_token is None
    assert result.page_size is None
