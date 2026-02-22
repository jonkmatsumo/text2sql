"""Golden simulation-mode coverage across tenant enforcement providers and modes."""

from __future__ import annotations

from unittest.mock import patch

import pytest

from tests.unit.policy.provider_mode_matrix import tenant_enforcement_provider_mode_rows


def _safe_sql_for_mode(mode: str, example_sql: dict[str, str]) -> str:
    return example_sql["safe_simple"] if mode == "sql_rewrite" else example_sql["not_required"]


@pytest.mark.asyncio
@pytest.mark.parametrize(("provider", "mode"), tenant_enforcement_provider_mode_rows())
async def test_simulation_mode_golden_matrix_outputs_are_stable(
    provider: str,
    mode: str,
    policy_factory,
    example_sql,
) -> None:
    """Simulation mode should emit stable decision metadata for each provider/mode pair."""
    policy = policy_factory(provider=provider, mode=mode)
    sql = _safe_sql_for_mode(mode, example_sql)
    tenant_id = 7

    with patch(
        "common.sql.tenant_sql_rewriter.transform_tenant_scoped_sql",
        side_effect=AssertionError("simulate=True should not call rewrite"),
    ):
        first = await policy.evaluate(
            sql=sql,
            tenant_id=tenant_id,
            params=[],
            tenant_column="tenant_id",
            global_table_allowlist=set(),
            simulate=True,
            schema_snapshot_loader=None,
        )
        second = await policy.evaluate(
            sql=sql,
            tenant_id=tenant_id,
            params=[],
            tenant_column="tenant_id",
            global_table_allowlist=set(),
            simulate=True,
            schema_snapshot_loader=None,
        )

    assert first.result == second.result
    assert first.bounded_reason_code == second.bounded_reason_code
    assert first.telemetry_attributes == second.telemetry_attributes

    telemetry = first.telemetry_attributes
    assert telemetry["tenant.policy.simulated"] is True
    assert telemetry["tenant.policy.simulation.provider"] == provider
    assert telemetry["tenant.policy.simulation.mode"] == mode
    assert telemetry["tenant.policy.simulation.reason"] == "NONE"
    assert telemetry["tenant.policy.simulation.reason_code"] == "none"
    if mode == "sql_rewrite":
        assert first.result.outcome == "APPLIED"
        assert telemetry["tenant.policy.simulation.decision"] == "REWRITE_REQUIRED"
        assert telemetry["tenant.policy.simulation.rewrite_reason_code"] == "none"
    else:
        assert first.result.outcome == "APPLIED"
        assert telemetry["tenant.policy.simulation.decision"] == "ALLOW"
        assert "tenant.policy.simulation.rewrite_reason_code" not in telemetry


@pytest.mark.asyncio
@pytest.mark.parametrize(("provider", "mode"), tenant_enforcement_provider_mode_rows())
async def test_simulation_mode_matrix_missing_tenant_denies_when_required(
    provider: str,
    mode: str,
    policy_factory,
    example_sql,
) -> None:
    """Simulation mode should fail closed with bounded reasons when tenant is required."""
    policy = policy_factory(provider=provider, mode=mode)
    sql = _safe_sql_for_mode(mode, example_sql)

    decision = await policy.evaluate(
        sql=sql,
        tenant_id=None,
        params=[],
        tenant_column="tenant_id",
        global_table_allowlist=set(),
        simulate=True,
        schema_snapshot_loader=None,
    )

    if mode == "sql_rewrite":
        assert decision.should_execute is False
        assert decision.result.outcome == "REJECTED_MISSING_TENANT"
        assert decision.bounded_reason_code == "tenant_rewrite_tenant_id_required"
        assert decision.telemetry_attributes["tenant.policy.simulation.decision"] == "DENY"
        assert (
            decision.telemetry_attributes["tenant.policy.simulation.rewrite_reason_code"]
            == "tenant_rewrite_tenant_id_required"
        )
    else:
        assert decision.should_execute is False
        assert decision.result.outcome == "REJECTED_MISSING_TENANT"
        assert decision.bounded_reason_code == "tenant_rewrite_tenant_id_required"
        assert decision.telemetry_attributes["tenant.policy.simulation.decision"] == "DENY"
        assert "tenant.policy.simulation.rewrite_reason_code" not in decision.telemetry_attributes
