"""Policy tests for tenant-id requirement decisions."""

import pytest


@pytest.mark.asyncio
async def test_policy_rejects_missing_tenant_when_rewrite_is_required(policy_factory, example_sql):
    """Missing tenant_id must be rejected when rewrite enforcement would apply."""
    policy = policy_factory()

    decision = await policy.evaluate(
        sql=example_sql["safe_simple"],
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
async def test_policy_skips_when_missing_tenant_but_enforcement_not_required(
    policy_factory, example_sql
):
    """Missing tenant_id should be allowed when enforcement is not required."""
    policy = policy_factory()

    decision = await policy.evaluate(
        sql=example_sql["not_required"],
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
