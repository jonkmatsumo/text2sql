import pytest

from dal.sqlite.param_translation import translate_postgres_params_to_sqlite


def test_translate_single_placeholder():
    """Translate a single $1 placeholder."""
    sql, params = translate_postgres_params_to_sqlite("SELECT $1 AS value", [10])
    assert sql == "SELECT ? AS value"
    assert params == [10]


def test_translate_reordered_placeholders():
    """Translate placeholders out of order while preserving arg mapping."""
    sql, params = translate_postgres_params_to_sqlite(
        "SELECT $2 AS b, $1 AS a", ["first", "second"]
    )
    assert sql == "SELECT ? AS b, ? AS a"
    assert params == ["second", "first"]


def test_translate_repeated_placeholder():
    """Duplicate args when the same placeholder is repeated."""
    sql, params = translate_postgres_params_to_sqlite(
        "SELECT * FROM t WHERE a = $1 OR b = $1", ["same"]
    )
    assert sql == "SELECT * FROM t WHERE a = ? OR b = ?"
    assert params == ["same", "same"]


def test_translate_invalid_index_zero():
    """Reject $0 placeholders."""
    with pytest.raises(ValueError, match="Invalid placeholder index"):
        translate_postgres_params_to_sqlite("SELECT $0", ["bad"])


def test_translate_gap_in_placeholders():
    """Reject gaps in placeholder sequences."""
    with pytest.raises(ValueError, match="without gaps"):
        translate_postgres_params_to_sqlite("SELECT $2", ["a", "b"])


def test_translate_missing_params():
    """Reject insufficient params for placeholders."""
    with pytest.raises(ValueError, match="Not enough parameters"):
        translate_postgres_params_to_sqlite("SELECT $1, $2", ["only-one"])


def test_translate_params_without_placeholders():
    """Reject params when no placeholders are present."""
    with pytest.raises(ValueError, match="no \\$N placeholders"):
        translate_postgres_params_to_sqlite("SELECT 1", ["unused"])
