import pytest

pytest.importorskip("aiomysql")

from dal.mysql.quoting import translate_double_quotes_to_backticks  # noqa: E402


def test_translate_double_quoted_identifiers():
    """Translate double-quoted identifiers to backticks."""
    sql = 'SELECT "users"."name" FROM "users"'
    assert translate_double_quotes_to_backticks(sql) == "SELECT `users`.`name` FROM `users`"


def test_preserve_double_quotes_in_strings():
    """Do not alter double quotes inside single-quoted strings."""
    sql = 'SELECT "name" FROM "users" WHERE note = \'He said "hi"\''
    expected = "SELECT `name` FROM `users` WHERE note = 'He said \"hi\"'"
    assert translate_double_quotes_to_backticks(sql) == expected


def test_handle_escaped_single_quotes():
    """Handle escaped single quotes without toggling quote state."""
    sql = "SELECT \"title\" FROM \"books\" WHERE note = 'O''Reilly'"
    expected = "SELECT `title` FROM `books` WHERE note = 'O''Reilly'"
    assert translate_double_quotes_to_backticks(sql) == expected
