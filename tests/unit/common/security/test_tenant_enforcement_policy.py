from common.security.tenant_enforcement_policy import (
    TenantEnforcementPolicy,
    TenantEnforcementResult,
    TenantSQLShape,
)


def test_tenant_enforcement_policy_decide_enforcement_sql_rewrite():
    """Test policy decision when mode is sql_rewrite."""
    policy = TenantEnforcementPolicy(
        provider="sqlite",
        mode="sql_rewrite",
        strict=True,
        max_targets=25,
        max_params=50,
        max_ast_nodes=1000,
        hard_timeout_ms=200,
    )
    assert policy.decide_enforcement() is True


def test_tenant_enforcement_policy_decide_enforcement_rls_session():
    """Test policy decision when mode is rls_session."""
    policy = TenantEnforcementPolicy(
        provider="postgres",
        mode="rls_session",
        strict=True,
        max_targets=25,
        max_params=50,
        max_ast_nodes=1000,
        hard_timeout_ms=200,
    )
    assert policy.decide_enforcement() is False


def test_tenant_enforcement_policy_decide_enforcement_none():
    """Test policy decision when mode is none."""
    policy = TenantEnforcementPolicy(
        provider="postgres",
        mode="none",
        strict=False,
        max_targets=25,
        max_params=50,
        max_ast_nodes=1000,
        hard_timeout_ms=200,
    )
    assert policy.decide_enforcement() is False


def test_policy_determine_outcome_rls_session():
    """Test outcome mapping when logic dictates rls_session is active."""
    policy = TenantEnforcementPolicy(
        provider="postgres",
        mode="rls_session",
        strict=True,
        max_targets=25,
        max_params=50,
        max_ast_nodes=100,
        hard_timeout_ms=200,
    )
    res = policy.determine_outcome(applied=False, reason_code=None)
    assert res == TenantEnforcementResult(
        applied=True, mode="rls_session", outcome="APPLIED", reason_code=None
    )


def test_policy_determine_outcome_none():
    """Test outcome mapping when no enforcement is needed."""
    policy = TenantEnforcementPolicy(
        provider="postgres",
        mode="none",
        strict=False,
        max_targets=25,
        max_params=50,
        max_ast_nodes=100,
        hard_timeout_ms=200,
    )
    res = policy.determine_outcome(applied=False, reason_code=None)
    assert res == TenantEnforcementResult(
        applied=False, mode="none", outcome="SKIPPED_NOT_REQUIRED", reason_code=None
    )


def test_policy_determine_outcome_sql_rewrite_applied():
    """Test outcome mapping when sql_rewrite applies perfectly."""
    policy = TenantEnforcementPolicy(
        provider="sqlite",
        mode="sql_rewrite",
        strict=True,
        max_targets=25,
        max_params=50,
        max_ast_nodes=100,
        hard_timeout_ms=200,
    )
    res = policy.determine_outcome(applied=True, reason_code=None)
    assert res == TenantEnforcementResult(
        applied=True, mode="sql_rewrite", outcome="APPLIED", reason_code=None
    )


def test_policy_determine_outcome_sql_rewrite_skipped():
    """Test outcome mapping when sql_rewrite skipped due to no predicates."""
    policy = TenantEnforcementPolicy(
        provider="sqlite",
        mode="sql_rewrite",
        strict=True,
        max_targets=25,
        max_params=50,
        max_ast_nodes=100,
        hard_timeout_ms=200,
    )
    res = policy.determine_outcome(applied=False, reason_code="NO_PREDICATES_PRODUCED")
    assert res == TenantEnforcementResult(
        applied=False, mode="sql_rewrite", outcome="SKIPPED_NOT_REQUIRED", reason_code=None
    )


def test_policy_determine_outcome_sql_rewrite_limit():
    """Test outcome mapping when limits are exceeded."""
    policy = TenantEnforcementPolicy(
        provider="sqlite",
        mode="sql_rewrite",
        strict=True,
        max_targets=25,
        max_params=50,
        max_ast_nodes=100,
        hard_timeout_ms=200,
    )
    res = policy.determine_outcome(applied=False, reason_code="AST_COMPLEXITY_EXCEEDED")
    assert res == TenantEnforcementResult(
        applied=False,
        mode="sql_rewrite",
        outcome="REJECTED_LIMIT",
        reason_code="AST_COMPLEXITY_EXCEEDED",
    )


def test_policy_determine_outcome_sql_rewrite_unsupported():
    """Test outcome mapping when shape is unsupported."""
    policy = TenantEnforcementPolicy(
        provider="sqlite",
        mode="sql_rewrite",
        strict=True,
        max_targets=25,
        max_params=50,
        max_ast_nodes=100,
        hard_timeout_ms=200,
    )
    res = policy.determine_outcome(applied=False, reason_code="CORRELATED_SUBQUERY_UNSUPPORTED")
    assert res == TenantEnforcementResult(
        applied=False,
        mode="sql_rewrite",
        outcome="REJECTED_UNSUPPORTED",
        reason_code="CORRELATED_SUBQUERY_UNSUPPORTED",
    )


def test_policy_classify_sql_safe_simple():
    """Test classification of a simple safe SELECT."""
    policy = TenantEnforcementPolicy(
        provider="sqlite",
        mode="sql_rewrite",
        strict=True,
        max_targets=25,
        max_params=50,
        max_ast_nodes=100,
        hard_timeout_ms=200,
    )
    shape = policy.classify_sql("SELECT id FROM users WHERE status = 'active'")
    assert shape == TenantSQLShape.SAFE_SIMPLE_SELECT


def test_policy_classify_sql_parse_error():
    """Test classification of invalid SQL."""
    policy = TenantEnforcementPolicy(
        provider="sqlite",
        mode="sql_rewrite",
        strict=True,
        max_targets=25,
        max_params=50,
        max_ast_nodes=100,
        hard_timeout_ms=200,
    )
    shape = policy.classify_sql("SELECT FROM WHERE")
    assert shape == TenantSQLShape.PARSE_ERROR


def test_policy_classify_sql_not_select():
    """Test classification of a non-SELECT statement."""
    policy = TenantEnforcementPolicy(
        provider="sqlite",
        mode="sql_rewrite",
        strict=True,
        max_targets=25,
        max_params=50,
        max_ast_nodes=100,
        hard_timeout_ms=200,
    )
    shape = policy.classify_sql("UPDATE users SET status = 'active'")
    assert shape == TenantSQLShape.UNSUPPORTED_STATEMENT_TYPE


def test_policy_classify_sql_set_operation():
    """Test classification of a UNION statement."""
    policy = TenantEnforcementPolicy(
        provider="sqlite",
        mode="sql_rewrite",
        strict=True,
        max_targets=25,
        max_params=50,
        max_ast_nodes=100,
        hard_timeout_ms=200,
    )
    shape = policy.classify_sql("SELECT id FROM a UNION SELECT id FROM b")
    assert shape == TenantSQLShape.UNSUPPORTED_SET_OPERATION


def test_policy_classify_sql_safe_cte():
    """Test classification of a simple CTE."""
    policy = TenantEnforcementPolicy(
        provider="sqlite",
        mode="sql_rewrite",
        strict=True,
        max_targets=25,
        max_params=50,
        max_ast_nodes=100,
        hard_timeout_ms=200,
    )
    shape = policy.classify_sql("WITH cte AS (SELECT id FROM users) SELECT id FROM cte")
    assert shape == TenantSQLShape.SAFE_CTE_QUERY


def test_policy_classify_sql_complexity_exceeded():
    """Test classification when AST node count exceeds limit."""
    policy = TenantEnforcementPolicy(
        provider="sqlite",
        mode="sql_rewrite",
        strict=True,
        max_targets=25,
        max_params=50,
        max_ast_nodes=2,  # Very low limit
        hard_timeout_ms=200,
    )
    shape = policy.classify_sql("SELECT id, name, status, role FROM users WHERE age > 21")
    assert shape == TenantSQLShape.UNSUPPORTED_COMPLEXITY


def test_policy_classify_sql_strict_mode_true():
    """Test classification rejects ambiguous queries in strict mode."""
    policy = TenantEnforcementPolicy(
        provider="sqlite",
        mode="sql_rewrite",
        strict=True,
        max_targets=25,
        max_params=50,
        max_ast_nodes=100,
        hard_timeout_ms=200,
    )

    # Just a simple correlated subquery to test the flag:
    shape = policy.classify_sql(
        "SELECT u.id FROM users u WHERE EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id)"
    )
    assert shape == TenantSQLShape.UNSUPPORTED_CORRELATED_SUBQUERY


def test_policy_classify_sql_strict_mode_false():
    """Test classification allows certain queries when strict mode is false."""
    policy = TenantEnforcementPolicy(
        provider="sqlite",
        mode="sql_rewrite",
        strict=False,
        max_targets=25,
        max_params=50,
        max_ast_nodes=100,
        hard_timeout_ms=200,
    )

    # With strict=False, an unqualified column in a subquery with only one table in scope
    # is NOT assumed to be correlated.
    sql = "SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)"
    shape = policy.classify_sql(sql)
    # The legacy strict mode false allows some queries that strict=True blocks.
    # Let's verify it doesn't return UNSUPPORTED_CORRELATED_SUBQUERY for this shape.
    assert shape == TenantSQLShape.SAFE_SIMPLE_SELECT
