"""Unit coverage for conservative tenant SQL rewrite."""

import pytest

from common.sql.tenant_sql_rewriter import TenantSQLRewriteError, rewrite_tenant_scoped_sql


@pytest.mark.parametrize(
    ("sql", "provider", "tenant_id", "expected_predicates", "expected_param_count"),
    [
        (
            "SELECT * FROM orders AS t",
            "sqlite",
            7,
            ["t.tenant_id = ?"],
            1,
        ),
        (
            "SELECT o.id, c.name FROM orders AS o JOIN customers AS c ON o.customer_id = c.id",
            "duckdb",
            42,
            ["o.tenant_id = ?", "c.tenant_id = ?"],
            2,
        ),
        (
            "SELECT o.id FROM orders o "
            "JOIN customers c ON o.customer_id = c.id "
            "JOIN regions r ON c.region_id = r.id",
            "sqlite",
            9,
            ["o.tenant_id = ?", "c.tenant_id = ?", "r.tenant_id = ?"],
            3,
        ),
    ],
)
def test_rewrite_applies_predicates_to_aliases_and_joins(
    sql: str,
    provider: str,
    tenant_id: int,
    expected_predicates: list[str],
    expected_param_count: int,
):
    """Rewrite should scope each eligible FROM/JOIN table using effective aliases."""
    result = rewrite_tenant_scoped_sql(
        sql,
        provider=provider,
        tenant_id=tenant_id,
    )

    for predicate in expected_predicates:
        assert predicate in result.rewritten_sql
    assert result.params == [tenant_id] * expected_param_count
    assert result.tenant_predicates_added == expected_param_count


def test_rewrite_existing_where_clause():
    """Existing WHERE clauses should be preserved and ANDed with tenant predicate."""
    result = rewrite_tenant_scoped_sql(
        "SELECT * FROM orders o WHERE o.status = 'open'",
        provider="sqlite",
        tenant_id=11,
    )

    assert "o.status = 'open' AND o.tenant_id = ?" in result.rewritten_sql
    assert result.params == [11]


def test_rewrite_rejects_nested_selects():
    """Nested subqueries are intentionally out of scope for v1 and must be rejected."""
    with pytest.raises(TenantSQLRewriteError, match="nested SELECTs"):
        rewrite_tenant_scoped_sql(
            "SELECT * FROM (SELECT * FROM orders) o",
            provider="sqlite",
            tenant_id=1,
        )


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT * FROM orders UNION SELECT * FROM archived_orders",
        "SELECT * FROM orders INTERSECT SELECT * FROM archived_orders",
        "SELECT * FROM orders EXCEPT SELECT * FROM archived_orders",
    ],
)
def test_rewrite_rejects_set_operations(sql: str):
    """Set operations are out of scope and must fail closed."""
    with pytest.raises(TenantSQLRewriteError, match="set operations"):
        rewrite_tenant_scoped_sql(
            sql,
            provider="sqlite",
            tenant_id=1,
        )


def test_rewrite_rejects_correlated_subqueries():
    """Correlated subqueries are rejected by the eligibility gate."""
    with pytest.raises(TenantSQLRewriteError, match="correlated subqueries"):
        rewrite_tenant_scoped_sql(
            (
                "SELECT * FROM orders o "
                "WHERE EXISTS (SELECT 1 FROM customers c WHERE c.id = o.customer_id)"
            ),
            provider="sqlite",
            tenant_id=1,
        )


def test_rewrite_rejects_window_functions():
    """Window functions are rejected until the rewriter can scope them safely."""
    with pytest.raises(TenantSQLRewriteError, match="window functions"):
        rewrite_tenant_scoped_sql(
            (
                "SELECT order_id, ROW_NUMBER() OVER "
                "(PARTITION BY customer_id ORDER BY created_at) AS rn "
                "FROM orders"
            ),
            provider="duckdb",
            tenant_id=1,
        )


def test_rewrite_rejects_ctes_by_default():
    """Common table expressions are intentionally fail-closed in v1."""
    with pytest.raises(TenantSQLRewriteError, match="CTEs"):
        rewrite_tenant_scoped_sql(
            "WITH scoped_orders AS (SELECT * FROM orders) SELECT * FROM scoped_orders",
            provider="sqlite",
            tenant_id=1,
        )


def test_rewrite_rejects_when_tenant_column_missing_in_schema_map():
    """Known schema without tenant column should fail safely."""
    with pytest.raises(TenantSQLRewriteError, match="Tenant column missing"):
        rewrite_tenant_scoped_sql(
            "SELECT * FROM orders",
            provider="sqlite",
            tenant_id=1,
            table_columns={"orders": ["id", "status"]},
        )
