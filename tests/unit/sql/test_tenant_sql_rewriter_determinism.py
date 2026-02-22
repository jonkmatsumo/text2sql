"""Determinism and idempotence contract tests for tenant SQL transformer."""

from __future__ import annotations

import pytest

from common.sql.tenant_sql_rewriter import (
    RewriteRequest,
    RewriteSuccess,
    assert_transform_idempotence,
    transform_tenant_scoped_sql,
)


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT o.id FROM orders o JOIN customers c ON c.id = o.customer_id",
        (
            "WITH recent_orders AS ("
            "SELECT o.id, o.customer_id FROM orders o"
            ") "
            "SELECT ro.id FROM recent_orders ro JOIN customers c ON c.id = ro.customer_id"
        ),
        "SELECT o.id FROM orders o WHERE o.total > (SELECT AVG(total) FROM orders)",
    ],
)
def test_transformer_is_deterministic_for_supported_shapes(sql: str) -> None:
    """Identical supported requests should produce identical rewrite output."""
    request = RewriteRequest(
        sql=sql,
        provider="sqlite",
        tenant_id=7,
        max_targets=25,
        max_params=50,
        max_ast_nodes=1000,
    )

    first = transform_tenant_scoped_sql(request)
    second = transform_tenant_scoped_sql(request)

    assert isinstance(first, RewriteSuccess)
    assert isinstance(second, RewriteSuccess)
    assert first.rewritten_sql == second.rewritten_sql
    assert first.params == second.params
    assert first.tables_rewritten == second.tables_rewritten
    assert first.tenant_predicates_added == second.tenant_predicates_added


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT o.id FROM orders o JOIN customers c ON c.id = o.customer_id",
        (
            "WITH recent_orders AS ("
            "SELECT o.id, o.customer_id FROM orders o"
            ") "
            "SELECT ro.id FROM recent_orders ro JOIN customers c ON c.id = ro.customer_id"
        ),
        "SELECT o.id FROM orders o WHERE o.total > (SELECT AVG(total) FROM orders)",
    ],
)
def test_transformer_is_idempotent_for_supported_shapes(sql: str) -> None:
    """Re-applying transform on rewritten SQL should not change SQL or add params."""
    request = RewriteRequest(
        sql=sql,
        provider="sqlite",
        tenant_id=7,
        max_targets=25,
        max_params=50,
        max_ast_nodes=1000,
    )
    assert_transform_idempotence(request)
