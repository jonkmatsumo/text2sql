from common.security.tenant_enforcement_policy import TenantEnforcementPolicy


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
