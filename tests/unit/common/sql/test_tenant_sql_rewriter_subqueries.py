import pytest
import sqlglot

from common.sql.tenant_sql_rewriter import (
    SubqueryClassification,
    TenantSQLRewriteError,
    classify_subquery,
    rewrite_tenant_scoped_sql,
)


def test_classify_safe_subquery_shapes():
    """Verify that acceptable subquery shapes are classified correctly."""
    # WHERE IN
    sql = "SELECT id FROM orders WHERE id IN (SELECT order_id FROM line_items)"
    expression = sqlglot.parse_one(sql)
    subquery = list(expression.find_all(sqlglot.exp.Select))[1]
    assert classify_subquery(subquery) == SubqueryClassification.SAFE_SIMPLE_SUBQUERY

    # EXISTS
    sql = (
        "SELECT id FROM orders WHERE EXISTS "
        "(SELECT 1 FROM line_items WHERE line_items.order_id = orders.id)"
    )
    expression = sqlglot.parse_one(sql)
    subquery = list(expression.find_all(sqlglot.exp.Select))[1]
    assert classify_subquery(subquery) == SubqueryClassification.SAFE_SIMPLE_SUBQUERY

    # FROM projection
    sql = "SELECT (SELECT count(*) FROM line_items) AS total FROM orders"
    expression = sqlglot.parse_one(sql)
    subquery = list(expression.find_all(sqlglot.exp.Select))[1]
    assert classify_subquery(subquery) == SubqueryClassification.SAFE_SIMPLE_SUBQUERY


def test_classify_rejects_subqueries_with_set_operations():
    """Verify that set operations in subqueries classify as unsupported."""
    sql = "SELECT id FROM orders WHERE id IN (SELECT id FROM a UNION SELECT id FROM b)"
    expression = sqlglot.parse_one(sql)
    subquery = list(expression.find_all(sqlglot.exp.Union))[0]
    assert classify_subquery(subquery) == SubqueryClassification.UNSUPPORTED_SUBQUERY


def test_classify_rejects_deeply_nested_subqueries():
    """Verify that a subquery containing another subquery is rejected."""
    sql = "SELECT id FROM orders WHERE id IN (SELECT id FROM a WHERE id IN (SELECT id FROM b))"
    expression = sqlglot.parse_one(sql)
    subquery = list(expression.find_all(sqlglot.exp.Select))[1]
    # The first subquery contains another, so it should be rejected.
    assert classify_subquery(subquery) == SubqueryClassification.UNSUPPORTED_SUBQUERY


def test_classify_rejects_ctes_in_subqueries():
    """Verify that CTEs inside subqueries classify as unsupported."""
    sql = "SELECT id FROM orders WHERE id IN (WITH c AS (SELECT id FROM b) SELECT id FROM c)"
    expression = sqlglot.parse_one(sql)
    subquery = list(expression.find_all(sqlglot.exp.Select))[1]
    assert classify_subquery(subquery) == SubqueryClassification.UNSUPPORTED_SUBQUERY


def test_rewrite_tenant_scoped_sql_rejects_unsupported_subquery():
    """Verify that top-level reject on unsupported subqueries functions properly."""
    with pytest.raises(TenantSQLRewriteError, match="set operations|subquery shape"):
        rewrite_tenant_scoped_sql(
            "SELECT id FROM orders WHERE id IN (SELECT id FROM a UNION SELECT id FROM b)",
            provider="sqlite",
            tenant_id=1,
        )


def test_rewrite_tenant_scoped_sql_rejects_deeply_nested_subquery():
    """Verify that top-level reject on deep subqueries functions properly."""
    with pytest.raises(TenantSQLRewriteError, match="not support this subquery shape"):
        rewrite_tenant_scoped_sql(
            "SELECT id FROM orders WHERE id IN (SELECT id FROM a WHERE id IN (SELECT id FROM b))",
            provider="sqlite",
            tenant_id=1,
        )


def test_rewrite_applies_predicates_to_subquery():
    """Verify tenant predicates are correctly applied to a subquery base table."""
    sql = "SELECT * FROM orders WHERE id IN (SELECT order_id FROM line_items)"
    result = rewrite_tenant_scoped_sql(sql, provider="sqlite", tenant_id=42)

    assert result.tenant_predicates_added == 2
    assert "orders.tenant_id = ?" in result.rewritten_sql
    assert "line_items.tenant_id = ?" in result.rewritten_sql
    assert result.params == [42, 42]
