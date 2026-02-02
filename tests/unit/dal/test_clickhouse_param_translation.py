import pytest

from dal.clickhouse.param_translation import translate_postgres_params_to_clickhouse


def test_translate_single_placeholder():
    """Translate a single $1 placeholder."""
    sql, params = translate_postgres_params_to_clickhouse("SELECT $1 AS value", [10])
    assert "{p1: Int64}" in sql
    assert params == {"p1": 10}


def test_translate_reordered_placeholders():
    """Translate placeholders out of order while preserving arg mapping."""
    sql, params = translate_postgres_params_to_clickhouse(
        "SELECT $2 AS b, $1 AS a", ["first", "second"]
    )
    assert "{p2: String}" in sql
    assert "{p1: String}" in sql
    assert params == {"p1": "first", "p2": "second"}


def test_translate_invalid_index_zero():
    """Reject $0 placeholders."""
    with pytest.raises(ValueError, match="Invalid placeholder index"):
        translate_postgres_params_to_clickhouse("SELECT $0", ["bad"])


def test_translate_gap_in_placeholders():
    """Reject gaps in placeholder sequences."""
    with pytest.raises(ValueError, match="without gaps"):
        translate_postgres_params_to_clickhouse("SELECT $2", ["a", "b"])


def test_translate_missing_params():
    """Reject insufficient params for placeholders."""
    with pytest.raises(ValueError, match="Not enough parameters"):
        translate_postgres_params_to_clickhouse("SELECT $1, $2", ["only-one"])


def test_translate_params_without_placeholders():
    """Reject params when no placeholders are present."""
    with pytest.raises(ValueError, match="no \\$N placeholders"):
        translate_postgres_params_to_clickhouse("SELECT 1", ["unused"])
