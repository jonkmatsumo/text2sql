import pytest
import sqlglot

from common.sql.tenant_sql_rewriter import CTEClassification, classify_cte_query


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
