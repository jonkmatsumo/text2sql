from datetime import date

import sqlglot

from schema.evaluation.metrics_v2_extractors import (
    extract_date_predicates,
    extract_equality_predicates,
    extract_in_lists,
    extract_limit_value,
    extract_numeric_predicates,
)


def test_extract_numeric_predicates():
    """Test standard numeric predicate extraction."""
    sql = "SELECT * FROM t WHERE price > 100 AND stock <= 50 AND age BETWEEN 20 AND 30"
    ast = sqlglot.parse_one(sql, read="postgres")
    res = extract_numeric_predicates(ast)

    assert ("price", ">", 100.0) in res
    assert ("stock", "<=", 50.0) in res
    assert ("age", ">=", 20.0) in res
    assert ("age", "<=", 30.0) in res
    assert len(res) == 4


def test_extract_numeric_predicates_swapped():
    """Test numeric extraction when column and literal are swapped."""
    sql = "SELECT * FROM t WHERE 500 < amount"
    ast = sqlglot.parse_one(sql, read="postgres")
    res = extract_numeric_predicates(ast)
    assert ("amount", ">", 500.0) in res


def test_extract_in_lists():
    """Test IN list extraction."""
    sql = "SELECT * FROM t WHERE status IN ('A', 'B') AND id IN (1, 2, 3)"
    ast = sqlglot.parse_one(sql, read="postgres")
    res = extract_in_lists(ast)

    # Order of results depends on find_all order
    status_match = next(r for r in res if r[0] == "status")
    id_match = next(r for r in res if r[0] == "id")

    assert status_match[1] == {"a", "b"}
    assert id_match[1] == {1.0, 2.0, 3.0}


def test_extract_equality_predicates():
    """Test equality predicate extraction."""
    sql = "SELECT * FROM t WHERE category = 'electronics' AND active = 1"
    ast = sqlglot.parse_one(sql, read="postgres")
    res = extract_equality_predicates(ast)

    assert ("category", "electronics") in res
    assert ("active", 1.0) in res


def test_extract_limit_value():
    """Test LIMIT value extraction."""
    assert extract_limit_value(sqlglot.parse_one("SELECT * FROM t LIMIT 10")) == 10
    assert extract_limit_value(sqlglot.parse_one("SELECT * FROM t")) is None


def test_extract_date_predicates():
    """Test date predicate extraction."""
    sql = "SELECT * FROM t WHERE created_at >= '2024-01-01' AND closed_at < '2025-01-01'"
    ast = sqlglot.parse_one(sql, read="postgres")
    res = extract_date_predicates(ast)

    assert ("created_at", ">=", date(2024, 1, 1)) in res
    assert ("closed_at", "<", date(2025, 1, 1)) in res


def test_extract_date_predicates_between():
    """Test date extraction from BETWEEN."""
    sql = "SELECT * FROM t WHERE event_date BETWEEN '2024-06-01' AND '2024-06-30'"
    ast = sqlglot.parse_one(sql, read="postgres")
    res = extract_date_predicates(ast)

    assert ("event_date", ">=", date(2024, 6, 1)) in res
    assert ("event_date", "<=", date(2024, 6, 30)) in res


def test_extract_nested_and_or():
    """Test nested AND/OR predicate extraction."""
    sql = "SELECT * FROM t WHERE (a > 10 OR b < 5) AND c = 'foo'"
    ast = sqlglot.parse_one(sql, read="postgres")

    num_res = extract_numeric_predicates(ast)
    eq_res = extract_equality_predicates(ast)

    assert ("a", ">", 10.0) in num_res
    assert ("b", "<", 5.0) in num_res
    assert ("c", "foo") in eq_res


def test_extract_non_parseable_values():
    """Test that non-literal values are ignored."""
    # Subqueries and functions should be ignored by literal extractors
    sql = "SELECT * FROM t WHERE price > (SELECT avg(p) FROM t2) AND status = upper('active')"
    ast = sqlglot.parse_one(sql, read="postgres")

    num_res = extract_numeric_predicates(ast)
    eq_res = extract_equality_predicates(ast)

    # Should be empty or at least not contain the subquery/function as a float/str
    assert len(num_res) == 0
    assert len(eq_res) == 0


def test_extract_multiple_predicates_same_column():
    """Test extraction of multiple predicates for the same column."""
    sql = "SELECT * FROM t WHERE price > 100 AND price < 200"
    ast = sqlglot.parse_one(sql, read="postgres")
    res = extract_numeric_predicates(ast)

    assert ("price", ">", 100.0) in res
    assert ("price", "<", 200.0) in res
    assert len(res) == 2
