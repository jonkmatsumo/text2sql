import pytest

from schema.evaluation.metrics_v2 import MetricSuiteV2


def test_metric_suite_v2_basic():
    """Test basic V2 composite scoring."""
    exp_sql = "SELECT * FROM t WHERE price > 100"

    # Perfect match
    gen_sql_1 = "SELECT * FROM t WHERE price > 100"
    res1 = MetricSuiteV2.compute_all(gen_sql_1, exp_sql)

    assert res1["metrics_version"] == "v2"
    assert res1["exact_match"] is True
    assert res1["structural_score_v1"] == 1.0
    assert res1["structural_score_v2"] == 1.0
    assert res1["value_aware_score"] == 1.0


def test_metric_suite_v2_regression_detection():
    """Test that V2 detects regressions V1 misses."""
    exp_sql = "SELECT * FROM t WHERE price > 100"

    # V1 misses this regression (both are 'range' predicates)
    gen_sql_v1_miss = "SELECT * FROM t WHERE price > 10"

    # structural_score_v1 should be the same as perfect match (since both have 1 table,
    # 1 range predicate)
    # Actually, in V1, predicate_similarity only checks the SET of types.
    # exp has {range}, gen has {range} -> 1.0 similarity.
    # So structural_score_v1 will be 1.0.

    res = MetricSuiteV2.compute_all(gen_sql_v1_miss, exp_sql)

    assert res["structural_score_v1"] == 1.0
    # numeric_range_similarity: 1 - |100-10|/100 = 0.1
    # total value_aware_score = 0.1 * 0.25 (weight) + 1.0 * (other weights...)
    # Actually, if other predicates are missing, they should score 0.0 or 1.0?
    # extractors return empty list if missing. subscores handle empty exp.

    # Let's check subscores
    assert res["v2_subscores"]["numeric_range_similarity"] == pytest.approx(0.1)
    # Other subscores should be 1.0 because exp_pred is empty for those categories
    assert res["v2_subscores"]["date_range_similarity"] == 1.0
    assert res["v2_subscores"]["set_overlap_similarity"] == 1.0
    assert res["v2_subscores"]["equality_value_match"] == 1.0
    assert res["v2_subscores"]["limit_distance_score"] == 1.0

    # Weighted value_aware_score:
    # 0.1 * 0.25 + 1.0 * (0.25 + 0.20 + 0.20 + 0.10)
    # = 0.025 + 0.75 = 0.775
    assert res["value_aware_score"] == pytest.approx(0.775)

    # Composite Score: 0.6 * 1.0 (V1) + 0.4 * 0.775 (V2)
    # = 0.6 + 0.31 = 0.91
    assert res["structural_score_v2"] == pytest.approx(0.91)


def test_metric_suite_v2_parsing_failure():
    """Test behavior when parsing fails."""
    exp_sql = "SELECT * FROM t WHERE price > 100"
    gen_sql_malformed = "SELECT * FROM t WHERE price > "

    res = MetricSuiteV2.compute_all(gen_sql_malformed, exp_sql)

    assert res["exact_match"] is False
    assert res["structural_score_v1"] == 0.0
    assert res["structural_score_v2"] == 0.0
    assert res["value_aware_score"] == 0.0


def test_metric_suite_v2_no_generated_sql():
    """Test behavior when generated SQL is missing."""
    exp_sql = "SELECT * FROM t WHERE price > 100"
    res = MetricSuiteV2.compute_all(None, exp_sql)

    assert res["structural_score_v2"] == 0.0
    assert res["value_aware_score"] == 0.0
