"""Drift guards for policy mode/provider coverage and result invariants."""

import inspect
import re

import pytest

from dal.capabilities import capabilities_for_provider
from tests._support.tenant_enforcement_contract import (
    VALID_TENANT_ENFORCEMENT_MODES,
    VALID_TENANT_ENFORCEMENT_OUTCOMES,
    assert_tenant_enforcement_contract,
)


def _assert_decision_invariants(decision) -> None:
    assert decision.result.mode
    assert decision.result.outcome in VALID_TENANT_ENFORCEMENT_OUTCOMES
    assert isinstance(decision.result.applied, bool)

    if decision.result.outcome.startswith("REJECTED_"):
        assert decision.result.reason_code is not None
    else:
        assert decision.result.reason_code is None

    assert_tenant_enforcement_contract(
        decision.envelope_metadata,
        decision,
        telemetry=decision.telemetry_attributes,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("provider", "mode", "sql", "tenant_id"),
    [
        ("sqlite", "sql_rewrite", "SELECT * FROM orders", 7),
        ("sqlite", "sql_rewrite", "SELECT 1 AS ok", None),
        ("sqlite", "sql_rewrite", "SELECT * FROM orders", None),
        ("postgres", "rls_session", "SELECT 1", 7),
        ("postgres", "none", "SELECT 1", None),
        ("mysql", "unsupported", "SELECT * FROM orders", 7),
    ],
)
async def test_policy_drift_guard_mode_result_invariants(
    provider: str,
    mode: str,
    sql: str,
    tenant_id: int | None,
    policy_factory,
):
    """Policy decision payload should preserve invariant envelope and telemetry contracts."""
    decision = await policy_factory(provider=provider, mode=mode).evaluate(
        sql=sql,
        tenant_id=tenant_id,
        params=[],
        tenant_column="tenant_id",
        global_table_allowlist=set(),
        schema_snapshot_loader=None,
    )
    _assert_decision_invariants(decision)


def test_policy_drift_guard_provider_capability_coverage(policy_factory):
    """Providers advertising tenant enforcement must map to policy-covered modes."""
    source = inspect.getsource(capabilities_for_provider)
    provider_names = set(re.findall(r'normalized == "([a-z0-9_]+)"', source))
    assert provider_names, "Provider discovery failed for capabilities_for_provider"

    covered = set()
    for provider in sorted(provider_names):
        caps = capabilities_for_provider(provider)
        if not caps.supports_tenant_enforcement:
            continue
        covered.add(provider)
        assert caps.tenant_enforcement_mode in {"sql_rewrite", "rls_session"}

        decision = policy_factory(
            provider=provider,
            mode=caps.tenant_enforcement_mode,
        ).default_decision(
            sql="SELECT 1",
            params=[],
        )
        assert (
            decision.envelope_metadata["tenant_enforcement_mode"] in VALID_TENANT_ENFORCEMENT_MODES
        )
        assert (
            decision.envelope_metadata["tenant_rewrite_outcome"]
            in VALID_TENANT_ENFORCEMENT_OUTCOMES
        )
        assert isinstance(decision.envelope_metadata["tenant_enforcement_applied"], bool)

    assert covered, "No tenant-enforcement providers discovered in capabilities registry"
