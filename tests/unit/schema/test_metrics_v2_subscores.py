import pytest
import sqlglot

from schema.evaluation.metrics_v2_subscores import (
    date_range_similarity,
    equality_value_match,
    limit_distance_score,
    numeric_range_similarity,
    set_overlap_similarity,
)


def test_numeric_range_similarity():
    """Test numeric range similarity calculation."""
    exp_sql = "SELECT * FROM t WHERE price > 100"

    # Perfect match
    gen_sql_1 = "SELECT * FROM t WHERE price > 100"
    # Small difference: 1 - |100-90|/100 = 0.9
    gen_sql_2 = "SELECT * FROM t WHERE price > 90"
    # Large difference: 1 - |100-10|/100 = 0.1
    gen_sql_3 = "SELECT * FROM t WHERE price > 10"
    # No match
    gen_sql_4 = "SELECT * FROM t WHERE price < 100"

    ast_exp = sqlglot.parse_one(exp_sql, read="postgres")

    assert numeric_range_similarity(sqlglot.parse_one(gen_sql_1, read="postgres"), ast_exp) == 1.0
    assert numeric_range_similarity(
        sqlglot.parse_one(gen_sql_2, read="postgres"), ast_exp
    ) == pytest.approx(0.9)
    assert numeric_range_similarity(
        sqlglot.parse_one(gen_sql_3, read="postgres"), ast_exp
    ) == pytest.approx(0.1)
    assert numeric_range_similarity(sqlglot.parse_one(gen_sql_4, read="postgres"), ast_exp) == 0.0


def test_date_range_similarity():
    """Test date range similarity calculation."""
    exp_sql = "SELECT * FROM t WHERE d = '2024-01-01'"

    # 10 days difference: 1 - 10/365 = 0.9726
    gen_sql_1 = "SELECT * FROM t WHERE d = '2024-01-11'"
    # 365+ days difference
    gen_sql_2 = "SELECT * FROM t WHERE d = '2025-01-01'"

    ast_exp = sqlglot.parse_one(exp_sql, read="postgres")

    score1 = date_range_similarity(sqlglot.parse_one(gen_sql_1, read="postgres"), ast_exp)
    assert score1 == pytest.approx(0.9726, abs=1e-4)

    score2 = date_range_similarity(sqlglot.parse_one(gen_sql_2, read="postgres"), ast_exp)
    assert score2 == 0.0


def test_set_overlap_similarity():
    """Test set overlap similarity calculation."""
    exp_sql = "SELECT * FROM t WHERE status IN ('A', 'B', 'C')"

    # Partial overlap: Jaccard = 2/4 = 0.5
    gen_sql_1 = "SELECT * FROM t WHERE status IN ('A', 'B', 'D')"
    # Subset: Jaccard = 2/3 = 0.66
    gen_sql_2 = "SELECT * FROM t WHERE status IN ('A', 'B')"

    ast_exp = sqlglot.parse_one(exp_sql, read="postgres")

    score1 = set_overlap_similarity(sqlglot.parse_one(gen_sql_1, read="postgres"), ast_exp)
    assert score1 == pytest.approx(0.5)

    score2 = set_overlap_similarity(sqlglot.parse_one(gen_sql_2, read="postgres"), ast_exp)
    assert score2 == pytest.approx(0.6666, abs=1e-4)


def test_equality_value_match():
    """Test equality value match calculation."""
    exp_sql = "SELECT * FROM t WHERE category = 'electronics'"
    gen_sql_1 = "SELECT * FROM t WHERE category = 'electronics'"
    gen_sql_2 = "SELECT * FROM t WHERE category = 'books'"

    ast_exp = sqlglot.parse_one(exp_sql, read="postgres")

    assert equality_value_match(sqlglot.parse_one(gen_sql_1, read="postgres"), ast_exp) == 1.0
    assert equality_value_match(sqlglot.parse_one(gen_sql_2, read="postgres"), ast_exp) == 0.0


def test_limit_distance_score():
    """Test limit distance score calculation."""
    exp_ast = sqlglot.parse_one("SELECT * FROM t LIMIT 10")

    assert limit_distance_score(sqlglot.parse_one("SELECT * FROM t LIMIT 10"), exp_ast) == 1.0
    # 1 - |100-10|/100 = 0.1
    assert limit_distance_score(
        sqlglot.parse_one("SELECT * FROM t LIMIT 100"), exp_ast
    ) == pytest.approx(0.1)
    # No limit
    assert limit_distance_score(sqlglot.parse_one("SELECT * FROM t"), exp_ast) == 0.0
