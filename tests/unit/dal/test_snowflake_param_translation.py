import sys

import pytest

pytest.importorskip("snowflake")

if sys.version_info < (3, 10):
    pytest.skip("Snowflake param typing requires Python 3.10+", allow_module_level=True)

from dal.snowflake.param_translation import translate_postgres_params_to_snowflake  # noqa: E402


def test_translate_single_placeholder():
    """Translate a single $1 placeholder."""
    sql, params = translate_postgres_params_to_snowflake("SELECT $1 AS value", [10])
    assert sql == "SELECT %(p1)s AS value"
    assert params == {"p1": 10}


def test_translate_reordered_placeholders():
    """Translate placeholders out of order while preserving arg mapping."""
    sql, params = translate_postgres_params_to_snowflake(
        "SELECT $2 AS b, $1 AS a", ["first", "second"]
    )
    assert sql == "SELECT %(p2)s AS b, %(p1)s AS a"
    assert params == {"p1": "first", "p2": "second"}


def test_translate_repeated_placeholder():
    """Reuse bindings when the same placeholder is repeated."""
    sql, params = translate_postgres_params_to_snowflake(
        "SELECT * FROM t WHERE a = $1 OR b = $1", ["same"]
    )
    assert sql == "SELECT * FROM t WHERE a = %(p1)s OR b = %(p1)s"
    assert params == {"p1": "same"}


def test_translate_invalid_index_zero():
    """Reject $0 placeholders."""
    with pytest.raises(ValueError, match="Invalid placeholder index"):
        translate_postgres_params_to_snowflake("SELECT $0", ["bad"])


def test_translate_gap_in_placeholders():
    """Reject gaps in placeholder sequences."""
    with pytest.raises(ValueError, match="without gaps"):
        translate_postgres_params_to_snowflake("SELECT $2", ["a", "b"])


def test_translate_missing_params():
    """Reject insufficient params for placeholders."""
    with pytest.raises(ValueError, match="Not enough parameters"):
        translate_postgres_params_to_snowflake("SELECT $1, $2", ["only-one"])


def test_translate_params_without_placeholders():
    """Reject params when no placeholders are present."""
    with pytest.raises(ValueError, match="no \\$N placeholders"):
        translate_postgres_params_to_snowflake("SELECT 1", ["unused"])
