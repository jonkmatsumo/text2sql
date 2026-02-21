from common.security.tenant_enforcement_policy import (
    TenantEnforcementPolicy,
    TenantEnforcementResult,
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
