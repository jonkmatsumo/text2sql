"""Tests for SQL evaluation metrics."""

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

    # Case insensitive identifiers
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


def test_parse_failure_handling():
    """Test behavior when generated SQL is invalid."""
    expected_sql = "SELECT * FROM users"
    invalid_sql = "SELECT FROM WHERE"

    results = MetricSuiteV1.compute_all(invalid_sql, expected_sql)

    assert results["exact_match"] is False
    assert results["structural_score"] == 0.0
    assert len(results["parse_errors"]) > 0
    assert results["expected_tables"] == ["users"]


def test_table_overlap():
    """Test table overlap subscore."""
    sql1 = "SELECT * FROM users, orders"
    sql2 = "SELECT * FROM users, products"

    results = MetricSuiteV1.compute_all(sql1, sql2)
    # tables: {users, orders} vs {users, products}
    # intersection: {users} (1)
    # union: {users, orders, products} (3)
    # score: 1/3 = 0.3333
    assert results["subscores"]["table_overlap"] == pytest.approx(0.3333, abs=1e-4)


def test_aggregation_match():
    """Test aggregation match subscore."""
    sql1 = "SELECT COUNT(*), SUM(val) FROM t"
    sql2 = "SELECT COUNT(*), AVG(val) FROM t"

    results = MetricSuiteV1.compute_all(sql1, sql2)
    # Aggs: {Count: 1, Sum: 1} vs {Count: 1, Avg: 1}
    # Intersection match: Count: 1
    # Total max: Count: 1, Sum: 1, Avg: 1 -> sum = 3
    # Score: 1/3 = 0.3333
    assert results["subscores"]["aggregation_match"] == pytest.approx(0.3333, abs=1e-4)


def test_groupby_match():
    """Test GROUP BY match subscore."""
    sql1 = "SELECT a, b FROM t GROUP BY a, b"
    sql2 = "SELECT a, b FROM t GROUP BY a"

    results = MetricSuiteV1.compute_all(sql1, sql2)
    # group cols: {a, b} vs {a}
    # jaccard: 1/2 = 0.5
    assert results["subscores"]["groupby_match"] == 0.5


def test_limit_match():
    """Test LIMIT match subscore."""
    sql1 = "SELECT * FROM t LIMIT 10"
    sql2 = "SELECT * FROM t LIMIT 20"
    sql3 = "SELECT * FROM t"

    assert MetricSuiteV1.compute_all(sql1, sql2)["subscores"]["limit_match"] == 0.0
    assert MetricSuiteV1.compute_all(sql1, sql1)["subscores"]["limit_match"] == 1.0
    assert MetricSuiteV1.compute_all(sql1, sql3)["subscores"]["limit_match"] == 0.0


def test_empty_generated_sql():
    """Test behavior with null/empty generated SQL."""
    results = MetricSuiteV1.compute_all(None, "SELECT * FROM users")
    assert results["exact_match"] is False
    assert results["structural_score"] == 0.0
    assert "Missing generated SQL" in results["parse_errors"]
