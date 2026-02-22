"""Tests for simulation mode in tenant enforcement policy evaluation."""

from unittest.mock import patch

import pytest

from common.security.tenant_enforcement_policy import TenantEnforcementPolicy


def _policy() -> TenantEnforcementPolicy:
    return TenantEnforcementPolicy(
        provider="sqlite",
        mode="sql_rewrite",
        strict=True,
        max_targets=25,
        max_params=50,
        max_ast_nodes=1000,
        hard_timeout_ms=200,
        warn_ms=50,
    )


@pytest.mark.asyncio
async def test_simulation_mode_reports_applied_without_rewrite_execution():
    """simulate=True should produce policy outcome without invoking the rewrite engine."""
    policy = _policy()
    sql = "SELECT o.id FROM orders o JOIN customers c ON c.id = o.customer_id"
    with patch(
        "common.sql.tenant_sql_rewriter.rewrite_tenant_scoped_sql",
        side_effect=AssertionError("simulate=True should not call rewrite"),
    ):
        decision = await policy.evaluate(
            sql=sql,
            tenant_id=7,
            params=["active"],
            tenant_column="tenant_id",
            global_table_allowlist=set(),
            simulate=True,
            schema_snapshot_loader=None,
        )

    assert decision.result.outcome == "APPLIED"
    assert decision.result.reason_code is None
    assert decision.should_execute is True
    assert decision.would_apply_rewrite is True
    assert decision.sql_to_execute == sql
    assert decision.params_to_bind == ["active"]
    assert decision.telemetry_attributes.get("tenant.policy.simulated") is True


@pytest.mark.asyncio
async def test_simulation_mode_matches_non_simulated_for_unsupported_shape():
    """simulate=True should preserve unsupported outcome mapping for invalid shapes."""
    policy = _policy()
    sql = "SELECT u.id FROM users u WHERE EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id)"

    simulated = await policy.evaluate(
        sql=sql,
        tenant_id=7,
        params=[],
        tenant_column="tenant_id",
        global_table_allowlist=set(),
        simulate=True,
        schema_snapshot_loader=None,
    )
    non_simulated = await policy.evaluate(
        sql=sql,
        tenant_id=7,
        params=[],
        tenant_column="tenant_id",
        global_table_allowlist=set(),
        simulate=False,
        schema_snapshot_loader=None,
    )

    assert simulated.result.outcome == "REJECTED_UNSUPPORTED"
    assert non_simulated.result.outcome == "REJECTED_UNSUPPORTED"
    assert simulated.bounded_reason_code == non_simulated.bounded_reason_code
    assert simulated.bounded_reason_code == "tenant_rewrite_correlated_subquery_unsupported"
    assert simulated.would_apply_rewrite is False
