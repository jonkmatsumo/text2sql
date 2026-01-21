"""Tests for SQL evaluation metrics V1 (Spec Compliant)."""

import pytest

from schema.evaluation.metrics import MetricSuiteV1


def test_metric_suite_weights_sum_to_one():
    """Ensure that the hardcoded weights sum to exactly 1.0."""
    total_weight = sum(MetricSuiteV1.WEIGHTS.values())
    assert total_weight == pytest.approx(1.0)


def test_exact_match_cases():
    """Test various exact match scenarios."""
    sql1 = "SELECT * FROM users WHERE id = 1"
    sql2 = "SELECT * FROM users WHERE id = 1"
    sql3 = "select * from USERS where ID = 1"
    sql4 = "SELECT * FROM users  WHERE  id=1"
    sql5 = "SELECT name, age FROM users"

    # Identical
    assert MetricSuiteV1.check_exact_match(sql1, sql2) is True

    # Case insensitive identifiers (normalized by sqlglot/lowercase)
    assert MetricSuiteV1.check_exact_match(sql1, sql3) is True

    # Whitespace
    assert MetricSuiteV1.check_exact_match(sql1, sql4) is True

    # Different SQL
    assert MetricSuiteV1.check_exact_match(sql1, sql5) is False


def test_structural_score_identical():
    """Identical SQL should have a structural score of 1.0."""
    sql = (
        "SELECT name FROM users JOIN orders ON users.id = orders.user_id "
        "WHERE orders.amount > 100 GROUP BY name LIMIT 10"
    )
    results = MetricSuiteV1.compute_all(sql, sql)

    assert results["exact_match"] is True
    assert results["structural_score"] == 1.0
    for score in results["subscores"].values():
        assert score == 1.0


def test_parse_failure_behavior_spec():
    """
    Test behavior when either SQL fails to parse (per spec).

    if either parse fails: structural_score = 1.0 if exact_match True else 0.0
    """
    valid_sql = "SELECT * FROM users"
    invalid_sql = "SELECT FROM WHERE"

    # One fails
    results = MetricSuiteV1.compute_all(invalid_sql, valid_sql)
    assert results["exact_match"] is False
    assert results["structural_score"] == 0.0
    assert len(results["parse_errors"]) > 0

    # Both fail but identical
    results = MetricSuiteV1.compute_all("!!!", "!!!")
    assert results["exact_match"] is True
    assert results["structural_score"] == 1.0
    assert len(results["parse_errors"]) == 2

    # Both fail and different
    results = MetricSuiteV1.compute_all("!!!", "@@@")
    assert results["exact_match"] is False
    assert results["structural_score"] == 0.0
    assert len(results["parse_errors"]) == 2


def test_join_similarity_formula():
    """Test normalized join count difference."""
    # Equal joins (1 vs 1)
    sql1 = "SELECT * FROM a JOIN b ON a.id = b.id"
    sql2 = "SELECT * FROM a JOIN c ON a.id = c.id"
    assert MetricSuiteV1.compute_all(sql1, sql2)["subscores"]["join_similarity"] == 1.0

    # Different joins (1 vs 0) -> 1 - |1-0|/1 = 0
    sql3 = "SELECT * FROM a"
    assert MetricSuiteV1.compute_all(sql1, sql3)["subscores"]["join_similarity"] == 0.0

    # Different joins (2 vs 1) -> 1 - |2-1|/2 = 0.5
    sql4 = "SELECT * FROM a JOIN b ON a.id = b.id JOIN c ON b.id = c.id"
    assert MetricSuiteV1.compute_all(sql4, sql1)["subscores"]["join_similarity"] == 0.5


def test_aggregation_match_boolean():
    """Test boolean aggregation presence match."""
    sql1 = "SELECT COUNT(*) FROM t"
    sql2 = "SELECT SUM(x) FROM t"
    sql3 = "SELECT * FROM t"

    # Both have agg
    assert MetricSuiteV1.compute_all(sql1, sql2)["subscores"]["aggregation_match"] == 1.0
    # One has agg, other doesn't
    assert MetricSuiteV1.compute_all(sql1, sql3)["subscores"]["aggregation_match"] == 0.0
    # Neither has agg
    sql4 = "SELECT name FROM t"
    assert MetricSuiteV1.compute_all(sql3, sql4)["subscores"]["aggregation_match"] == 1.0


def test_groupby_match_boolean():
    """Test boolean GROUP BY presence match."""
    sql1 = "SELECT a FROM t GROUP BY a"
    sql2 = "SELECT b FROM t GROUP BY b"
    sql3 = "SELECT a FROM t"

    # Both have GB
    assert MetricSuiteV1.compute_all(sql1, sql2)["subscores"]["groupby_match"] == 1.0
    # One has GB
    assert MetricSuiteV1.compute_all(sql1, sql3)["subscores"]["groupby_match"] == 0.0
    # Neither has GB
    assert MetricSuiteV1.compute_all(sql3, "SELECT * FROM t")["subscores"]["groupby_match"] == 1.0


def test_predicate_similarity_types():
    """Test predicate type set Jaccard."""
    sql1 = "SELECT * FROM t WHERE a = 1 AND b > 2"  # types: {equality, range}
    sql2 = "SELECT * FROM t WHERE a = 1"  # types: {equality}
    sql3 = "SELECT * FROM t WHERE b BETWEEN 1 AND 5"  # types: {range}
    sql4 = "SELECT * FROM t WHERE c IN (1,2)"  # types: {in}

    # {eq, range} vs {eq} -> intersection 1, union 2 -> 0.5
    assert MetricSuiteV1.compute_all(sql1, sql2)["subscores"]["predicate_similarity"] == 0.5

    # {eq, range} vs {range} -> intersection 1, union 2 -> 0.5
    assert MetricSuiteV1.compute_all(sql1, sql3)["subscores"]["predicate_similarity"] == 0.5

    # {eq} vs {in} -> 0.0
    assert MetricSuiteV1.compute_all(sql2, sql4)["subscores"]["predicate_similarity"] == 0.0


def test_limit_match_spec():
    """Test LIMIT match formula and cases."""
    # Both None
    sql1 = "SELECT * FROM t"
    sql2 = "SELECT * FROM t"
    assert MetricSuiteV1.compute_all(sql1, sql2)["subscores"]["limit_match"] == 1.0

    # One None
    sql3 = "SELECT * FROM t LIMIT 10"
    assert MetricSuiteV1.compute_all(sql1, sql3)["subscores"]["limit_match"] == 0.0

    # Equal
    sql4 = "SELECT * FROM t LIMIT 10"
    assert MetricSuiteV1.compute_all(sql3, sql4)["subscores"]["limit_match"] == 1.0

    # Different: 1 - |10-20|/20 = 1 - 10/20 = 0.5
    sql5 = "SELECT * FROM t LIMIT 20"
    assert MetricSuiteV1.compute_all(sql3, sql5)["subscores"]["limit_match"] == 0.5


def test_structural_score_weighting():
    """Verify structural_score is the weighted sum of subscores."""
    sql1 = "SELECT a FROM t WHERE a = 1"
    sql2 = "SELECT a FROM t LIMIT 10"

    results = MetricSuiteV1.compute_all(sql1, sql2)
    sub = results["subscores"]
    weights = MetricSuiteV1.WEIGHTS

    expected = sum(sub[k] * weights[k] for k in weights)
    assert results["structural_score"] == pytest.approx(expected, abs=1e-4)
