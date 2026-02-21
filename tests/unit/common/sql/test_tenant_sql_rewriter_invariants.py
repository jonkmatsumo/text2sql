import pytest

from common.sql.tenant_sql_rewriter import assert_rewrite_invariants, rewrite_tenant_scoped_sql


def test_assert_rewrite_invariants_detects_missing_target_predicate():
    """Invariant harness should fail when an eligible target lacks tenant predicate."""
    with pytest.raises(AssertionError, match="Eligible rewrite target missing tenant predicate"):
        assert_rewrite_invariants(
            "SELECT * FROM orders",
            "SELECT * FROM orders",
            [],
            provider="sqlite",
            tenant_id=1,
        )


def test_assert_rewrite_invariants_detects_duplicate_scope_predicate():
    """Invariant harness should reject duplicate tenant predicates in one scope."""
    with pytest.raises(AssertionError, match="Duplicate tenant predicate"):
        assert_rewrite_invariants(
            "SELECT * FROM orders o",
            "SELECT * FROM orders AS o WHERE o.tenant_id = ? AND o.tenant_id = ?",
            [1, 1],
            provider="sqlite",
            tenant_id=1,
        )


def test_assert_rewrite_invariants_accepts_valid_rewrite_result():
    """Valid rewrites should satisfy all invariant checks."""
    sql = (
        "SELECT o.id FROM orders o "
        "WHERE EXISTS (SELECT 1 FROM customers c WHERE c.status = 'active')"
    )
    result = rewrite_tenant_scoped_sql(sql, provider="sqlite", tenant_id=7)

    assert_rewrite_invariants(
        sql,
        result.rewritten_sql,
        result.params,
        provider="sqlite",
        tenant_id=7,
    )


def test_rewrite_invokes_invariant_harness_in_test_mode(monkeypatch):
    """Debug/test mode should execute invariant checks on successful rewrites."""
    from common.sql import tenant_sql_rewriter

    def _raise_invariant(*args, **kwargs):
        del args, kwargs
        raise AssertionError("invariant-invoked")

    monkeypatch.setenv("TENANT_REWRITE_ASSERT_INVARIANTS", "true")
    monkeypatch.setattr(tenant_sql_rewriter, "assert_rewrite_invariants", _raise_invariant)

    with pytest.raises(AssertionError, match="invariant-invoked"):
        tenant_sql_rewriter.rewrite_tenant_scoped_sql(
            "SELECT * FROM orders",
            provider="sqlite",
            tenant_id=1,
        )
