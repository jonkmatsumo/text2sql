"""Tests for SQL comment stripping utilities."""

from common.sql.comments import strip_sql_comments


def test_strip_sql_comments_removes_line_and_block_comments():
    """Line and block comments should be removed from SQL text."""
    sql = "SELECT 1 -- comment\n/* block */ SELECT 2"
    stripped = strip_sql_comments(sql)
    assert "-- comment" not in stripped
    assert "/* block */" not in stripped
    assert "SELECT 1" in stripped
    assert "SELECT 2" in stripped


def test_strip_sql_comments_preserves_quoted_literals():
    """Comment-like text in quoted literals should be preserved."""
    sql = "SELECT '-- not comment', '/* not comment */'"
    stripped = strip_sql_comments(sql)
    assert stripped == sql
