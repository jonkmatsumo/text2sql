import pytest
import sqlglot

from common.sql.tenant_sql_rewriter import (
    CTEClassification,
    TenantSQLRewriteError,
    classify_cte_query,
    rewrite_tenant_scoped_sql,
)


@pytest.mark.parametrize(
    "sql, expected",
    [
        # SAFE cases
        (
            "WITH cte1 AS (SELECT * FROM orders) SELECT * FROM cte1",
            CTEClassification.SAFE_SIMPLE_CTE,
        ),
        (
            (
                "WITH cte1 AS (SELECT * FROM orders), "
                "cte2 AS (SELECT * FROM customers) "
                "SELECT * FROM cte1 JOIN cte2 ON cte1.customer_id = cte2.id"
            ),
            CTEClassification.SAFE_SIMPLE_CTE,
        ),
        (
            "WITH cte1 AS (SELECT id FROM orders WHERE status = 'open') SELECT * FROM cte1",
            CTEClassification.SAFE_SIMPLE_CTE,
        ),
        # REJECT cases
        (
            (
                "WITH RECURSIVE cte1 AS (SELECT 1 AS n UNION ALL "
                "SELECT n + 1 FROM cte1 WHERE n < 5) SELECT * FROM cte1"
            ),
            CTEClassification.UNSUPPORTED_CTE,
        ),
        (
            (
                "WITH cte1 AS (SELECT * FROM orders "
                "UNION SELECT * FROM archived_orders) SELECT * FROM cte1"
            ),
            CTEClassification.UNSUPPORTED_CTE,
        ),
        (
            "WITH cte1 AS (SELECT * FROM (SELECT * FROM orders) sub) SELECT * FROM cte1",
            CTEClassification.UNSUPPORTED_CTE,
        ),
        (
            "WITH cte1 AS (SELECT * FROM orders) SELECT * FROM (SELECT * FROM cte1) sub",
            CTEClassification.UNSUPPORTED_CTE,
        ),
        (
            (
                "WITH cte1 AS (SELECT * FROM orders "
                "WHERE id IN (SELECT order_id FROM details)) SELECT * FROM cte1"
            ),
            CTEClassification.UNSUPPORTED_CTE,
        ),
        (
            "SELECT * FROM orders",  # Not a CTE query
            CTEClassification.UNSUPPORTED_CTE,
        ),
    ],
)
def test_classify_cte_query(sql, expected):
    """Verify classification of various CTE query shapes."""
    expression = sqlglot.parse_one(sql)
    assert classify_cte_query(expression) == expected


def test_classify_cte_chained_interpretation():
    """Verify that chained CTEs are currently rejected as conservative policy."""
    # Testing the interpretation of "Each CTE body is a simple SELECT over base tables"
    # If CTE2 references CTE1, is it safe?
    # Current implementation says NO (UNSUPPORTED) because it only allows base tables in CTE bodies.
    sql = "WITH cte1 AS (SELECT * FROM orders), cte2 AS (SELECT * FROM cte1) SELECT * FROM cte2"
    expression = sqlglot.parse_one(sql)
    # Based on current implementation which is conservative:
    assert classify_cte_query(expression) == CTEClassification.UNSUPPORTED_CTE


@pytest.mark.parametrize(
    "sql, tenant_id, expected_sql_contains, expected_param_count",
    [
        (
            "WITH cte1 AS (SELECT * FROM orders) SELECT * FROM cte1",
            7,
            [
                "WITH cte1 AS (SELECT * FROM orders WHERE orders.tenant_id = ?)",
                "SELECT * FROM cte1",
            ],
            1,
        ),
        (
            (
                "WITH cte1 AS (SELECT * FROM orders) "
                "SELECT * FROM cte1 JOIN customers c ON cte1.customer_id = c.id"
            ),
            42,
            [
                "WITH cte1 AS (SELECT * FROM orders WHERE orders.tenant_id = ?)",
                (
                    "SELECT * FROM cte1 JOIN customers AS c ON cte1.customer_id = c.id "
                    "WHERE c.tenant_id = ?"
                ),
            ],
            2,
        ),
        (
            ("WITH cte1 AS (SELECT * FROM orders WHERE status = 'open') " "SELECT * FROM cte1"),
            11,
            ["WHERE status = 'open' AND orders.tenant_id = ?"],
            1,
        ),
    ],
)
def test_rewrite_tenant_scoped_sql_with_ctes(
    sql, tenant_id, expected_sql_contains, expected_param_count
):
    """Verify that rewrite applies predicates correctly to CTE bodies and main query."""
    result = rewrite_tenant_scoped_sql(sql, provider="sqlite", tenant_id=tenant_id)
    for expected in expected_sql_contains:
        # Normalize whitespace for comparison if needed, but sqlglot output is stable
        assert expected in result.rewritten_sql
    assert result.params == [tenant_id] * expected_param_count
    assert result.tenant_predicates_added == expected_param_count


def test_rewrite_ctes_with_global_tables():
    """Verify that global tables in CTEs are exempt from injection."""
    sql = "WITH cte1 AS (SELECT * FROM global_ref) SELECT * FROM cte1 JOIN orders o"
    result = rewrite_tenant_scoped_sql(
        sql,
        provider="sqlite",
        tenant_id=5,
        global_table_allowlist={"global_ref"},
    )
    assert "global_ref.tenant_id = ?" not in result.rewritten_sql
    assert "o.tenant_id = ?" in result.rewritten_sql
    assert result.params == [5]


def test_rewrite_ctes_fails_on_missing_column():
    """Verify that missing tenant column in a CTE table fails the entire rewrite."""
    sql = "WITH cte1 AS (SELECT * FROM orders) SELECT * FROM cte1"
    with pytest.raises(TenantSQLRewriteError, match="Tenant column missing"):
        rewrite_tenant_scoped_sql(
            sql,
            provider="sqlite",
            tenant_id=1,
            table_columns={"orders": ["id", "status"]},  # missing tenant_id
        )
