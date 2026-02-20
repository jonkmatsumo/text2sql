"""Unit coverage for conservative tenant SQL rewrite."""

import pytest

from common.sql.tenant_sql_rewriter import TenantSQLRewriteError, rewrite_tenant_scoped_sql


def test_rewrite_simple_from_sqlite():
    """A basic FROM query should get one tenant predicate and one tenant param."""
    result = rewrite_tenant_scoped_sql(
        "SELECT * FROM orders",
        provider="sqlite",
        tenant_id=7,
    )

    assert "orders.tenant_id = ?" in result.rewritten_sql
    assert result.params == [7]
    assert result.tenant_predicates_added == 1


def test_rewrite_join_with_aliases_duckdb():
    """JOIN queries should include one tenant predicate per table alias."""
    result = rewrite_tenant_scoped_sql(
        "SELECT o.id, c.name FROM orders o JOIN customers c ON o.customer_id = c.id",
        provider="duckdb",
        tenant_id=42,
    )

    assert "o.tenant_id = ?" in result.rewritten_sql
    assert "c.tenant_id = ?" in result.rewritten_sql
    assert result.params == [42, 42]
    assert result.tenant_predicates_added == 2


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


def test_rewrite_rejects_when_tenant_column_missing_in_schema_map():
    """Known schema without tenant column should fail safely."""
    with pytest.raises(TenantSQLRewriteError, match="Tenant column missing"):
        rewrite_tenant_scoped_sql(
            "SELECT * FROM orders",
            provider="sqlite",
            tenant_id=1,
            table_columns={"orders": ["id", "status"]},
        )
