"""Policy tests for tenant-id requirement decisions."""

import pytest

from common.security.tenant_enforcement_policy import TenantEnforcementPolicy


def _sql_rewrite_policy() -> TenantEnforcementPolicy:
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
async def test_policy_rejects_missing_tenant_when_rewrite_is_required():
    """Missing tenant_id must be rejected when rewrite enforcement would apply."""
    policy = _sql_rewrite_policy()

    decision = await policy.evaluate(
        sql="SELECT * FROM orders",
        tenant_id=None,
        params=[],
        tenant_column="tenant_id",
        global_table_allowlist=set(),
        schema_snapshot_loader=None,
    )

    assert decision.tenant_required is True
    assert decision.should_execute is False
    assert decision.result.outcome == "REJECTED_MISSING_TENANT"
    assert decision.result.reason_code == "TENANT_ID_REQUIRED"
    assert decision.bounded_reason_code == "tenant_rewrite_tenant_id_required"
    assert decision.envelope_metadata["tenant_rewrite_outcome"] == "REJECTED_MISSING_TENANT"
    assert (
        decision.envelope_metadata["tenant_rewrite_reason_code"]
        == "tenant_rewrite_tenant_id_required"
    )


@pytest.mark.asyncio
async def test_policy_skips_when_missing_tenant_but_enforcement_not_required():
    """Missing tenant_id should be allowed when enforcement is not required."""
    policy = _sql_rewrite_policy()

    decision = await policy.evaluate(
        sql="SELECT 1 AS ok",
        tenant_id=None,
        params=[],
        tenant_column="tenant_id",
        global_table_allowlist=set(),
        schema_snapshot_loader=None,
    )

    assert decision.tenant_required is False
    assert decision.should_execute is True
    assert decision.result.outcome == "SKIPPED_NOT_REQUIRED"
    assert decision.result.reason_code is None
    assert decision.envelope_metadata["tenant_rewrite_outcome"] == "SKIPPED_NOT_REQUIRED"
    assert decision.envelope_metadata.get("tenant_rewrite_reason_code") is None
