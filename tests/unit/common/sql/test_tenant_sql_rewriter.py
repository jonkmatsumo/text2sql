"""Unit coverage for conservative tenant SQL rewrite."""

import pytest

from common.sql.tenant_sql_rewriter import (
    MAX_TENANT_REWRITE_TARGETS,
    TenantSQLRewriteError,
    rewrite_tenant_scoped_sql,
)


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


def test_rewrite_rejects_unsupported_ctes():
    """Unsupported common table expressions must still fail closed."""
    with pytest.raises(TenantSQLRewriteError, match="set operations"):
        rewrite_tenant_scoped_sql(
            "WITH RECURSIVE cte1 AS (SELECT 1 UNION ALL SELECT 1 FROM cte1) SELECT * FROM cte1",
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


def test_rewrite_allows_explicitly_global_tables_without_tenant_predicates():
    """Allowlisted global tables should pass without tenant predicates."""
    result = rewrite_tenant_scoped_sql(
        "SELECT * FROM global_reference",
        provider="sqlite",
        tenant_id=1,
        global_table_allowlist={"global_reference"},
    )

    assert "tenant_id = ?" not in result.rewritten_sql
    assert result.params == []
    assert result.tenant_predicates_added == 0
    assert result.tables_rewritten == []


def test_rewrite_still_scopes_non_global_tables_when_allowlist_is_present():
    """Allowlist should only exempt listed tables and still scope the rest."""
    result = rewrite_tenant_scoped_sql(
        "SELECT o.id FROM global_reference g JOIN orders o ON g.order_id = o.id",
        provider="duckdb",
        tenant_id=7,
        global_table_allowlist={"global_reference"},
    )

    assert "g.tenant_id = ?" not in result.rewritten_sql
    assert "o.tenant_id = ?" in result.rewritten_sql
    assert result.params == [7]
    assert result.tenant_predicates_added == 1
    assert result.tables_rewritten == ["orders"]


def test_rewrite_determinism_stable_ordering():
    """Verify that multiple tables across CTEs yield deterministic param ordering."""
    sql = """
    WITH cte_b AS (SELECT * FROM table_b),
         cte_a AS (SELECT * FROM table_a)
    SELECT * FROM cte_a JOIN cte_b JOIN table_c
    """
    # Sorted order should be: table_a (in cte_a), table_b (in cte_b),
    # table_c (in final select)
    # cte_name sort: "", "cte_a", "cte_b"
    # Actually wait, my sort key is:
    # (cte_name or "", effective_name, physical_name, appearance_index)
    # "" (final select) comes first.
    # "cte_a" comes second.
    # "cte_b" comes third.

    # Final select: table_c
    # cte_a: table_a
    # cte_b: table_b

    # Let's adjust expected to match my sorting rule.
    # ( "", "table_c", ... )
    # ( "cte_a", "table_a", ... )
    # ( "cte_b", "table_b", ... )

    result1 = rewrite_tenant_scoped_sql(sql, provider="sqlite", tenant_id=100)
    result2 = rewrite_tenant_scoped_sql(sql, provider="sqlite", tenant_id=100)

    assert result1.rewritten_sql == result2.rewritten_sql
    assert result1.params == result2.params
    assert result1.tables_rewritten == ["table_c", "table_a", "table_b"]
    assert result1.tenant_predicates_added == 3


def test_rewrite_determinism_same_table_multiple_times():
    """Verify that the same table used multiple times has stable ordering via appearance_index."""
    sql = "SELECT * FROM orders o1 JOIN orders o2 ON o1.id = o2.id"
    # effective names: o1, o2
    # both in final select ("")
    # order: o1, o2

    result = rewrite_tenant_scoped_sql(sql, provider="sqlite", tenant_id=5)
    assert result.tables_rewritten == ["orders", "orders"]
    # Check that placeholders are in the right order in the generated SQL
    # sqlglot might generate something like WHERE o1.tenant_id = ? AND o2.tenant_id = ?
    assert "o1.tenant_id = ? AND o2.tenant_id = ?" in result.rewritten_sql


def test_rewrite_target_limit_boundary():
    """Verify that hitting the exact target limit succeeds."""
    joins = " ".join(
        f"JOIN orders o{i} ON o.id = o{i}.id" for i in range(1, MAX_TENANT_REWRITE_TARGETS)
    )
    sql = f"SELECT * FROM orders o {joins}"

    result = rewrite_tenant_scoped_sql(
        sql,
        provider="sqlite",
        tenant_id=1,
    )
    assert result.tenant_predicates_added == MAX_TENANT_REWRITE_TARGETS


def test_rewrite_target_limit_exceeded():
    """Verify that exceeding the target limit fails closed."""
    joins = " ".join(
        f"JOIN orders o{i} ON o.id = o{i}.id" for i in range(1, MAX_TENANT_REWRITE_TARGETS + 1)
    )
    sql = f"SELECT * FROM orders o {joins}"

    with pytest.raises(
        TenantSQLRewriteError, match="Tenant rewrite exceeded maximum allowed targets"
    ):
        rewrite_tenant_scoped_sql(
            sql,
            provider="sqlite",
            tenant_id=1,
        )


def test_rewrite_param_limit_exceeded():
    """Verify that exceeding the parameter limit fails closed."""
    from common.sql import tenant_sql_rewriter

    original_max = tenant_sql_rewriter.MAX_TENANT_REWRITE_PARAMS
    tenant_sql_rewriter.MAX_TENANT_REWRITE_PARAMS = 2

    try:
        sql = (
            "SELECT * FROM orders o1 "
            "JOIN orders o2 ON o1.id = o2.id "
            "JOIN orders o3 ON o1.id = o3.id"
        )
        with pytest.raises(
            TenantSQLRewriteError, match="Tenant rewrite exceeded maximum allowed parameters"
        ):
            rewrite_tenant_scoped_sql(
                sql,
                provider="sqlite",
                tenant_id=1,
            )
    finally:
        tenant_sql_rewriter.MAX_TENANT_REWRITE_PARAMS = original_max


def test_rewrite_single_node_dedup():
    """Assert de-dup works inside a single scope (no double injection)."""
    sql = "SELECT * FROM orders"
    result = rewrite_tenant_scoped_sql(sql, provider="sqlite", tenant_id=1)
    assert result.tenant_predicates_added == 1
    assert result.rewritten_sql.count("tenant_id = ?") == 1


def test_rewrite_correlation_shadowing():
    """Outer alias shadowed by inner alias. All refs qualify to inner."""
    sql = "SELECT * FROM orders o WHERE EXISTS (SELECT 1 FROM customers o WHERE o.id = 1)"
    result = rewrite_tenant_scoped_sql(sql, provider="sqlite", tenant_id=1)
    assert result.tenant_predicates_added == 2


def test_rewrite_correlation_ambiguous_unqualified():
    """Both scopes have `o` available. Unqualified column `id` -> reject as ambiguous."""
    sql = "SELECT * FROM orders o WHERE EXISTS (SELECT 1 FROM customers o WHERE id = 1)"
    with pytest.raises(TenantSQLRewriteError, match="correlated subqueries"):
        rewrite_tenant_scoped_sql(sql, provider="sqlite", tenant_id=1)


def test_rewrite_correlation_qualified_outer_ref():
    """Qualified outer alias reference inside subquery without inner `o` -> reject as correlated."""
    sql = "SELECT * FROM orders o WHERE EXISTS (SELECT 1 FROM customers c WHERE c.id = o.id)"
    with pytest.raises(TenantSQLRewriteError, match="correlated subqueries"):
        rewrite_tenant_scoped_sql(sql, provider="sqlite", tenant_id=1)


def test_rewrite_correlation_cte_collision_ambiguous():
    """CTE name collision with base table name in subquery, used unqualified."""
    sql = (
        "WITH t AS (SELECT * FROM archived) "
        "SELECT * FROM users WHERE EXISTS (SELECT 1 FROM customers t WHERE id = 1)"
    )
    with pytest.raises(TenantSQLRewriteError, match="correlated subqueries"):
        rewrite_tenant_scoped_sql(sql, provider="sqlite", tenant_id=1)
