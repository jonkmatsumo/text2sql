"""Conformance harness for tenant enforcement providers and modes."""

from __future__ import annotations

import inspect
import re

import pytest

from dal.capabilities import capabilities_for_provider
from tests._support.tenant_enforcement_contract import assert_tenant_enforcement_contract


def _tenant_enforcement_provider_modes() -> list[tuple[str, str]]:
    source = inspect.getsource(capabilities_for_provider)
    provider_names = sorted(set(re.findall(r'normalized == "([a-z0-9_]+)"', source)))
    pairs: list[tuple[str, str]] = []
    for provider in provider_names:
        caps = capabilities_for_provider(provider)
        if caps.supports_tenant_enforcement:
            pairs.append((provider, caps.tenant_enforcement_mode))
    assert pairs, "No tenant-enforcement providers discovered in capability registry"
    return pairs


def _safe_outcome_for_mode(mode: str) -> str:
    normalized = (mode or "").strip().lower()
    if normalized in {"sql_rewrite", "rls_session"}:
        return "APPLIED"
    if normalized == "none":
        return "SKIPPED_NOT_REQUIRED"
    return "REJECTED_UNSUPPORTED"


@pytest.mark.asyncio
@pytest.mark.parametrize(("provider", "mode"), _tenant_enforcement_provider_modes())
async def test_policy_conformance_provider_mode(
    provider: str,
    mode: str,
    policy_factory,
    example_sql,
) -> None:
    """Each provider/mode pair must satisfy core policy conformance scenarios."""
    policy = policy_factory(provider=provider, mode=mode)

    safe_sql = example_sql["safe_simple"] if mode == "sql_rewrite" else example_sql["not_required"]
    missing_tenant_decision = await policy.evaluate(
        sql=safe_sql,
        tenant_id=None,
        params=[],
        tenant_column="tenant_id",
        global_table_allowlist=set(),
        schema_snapshot_loader=None,
    )
    assert missing_tenant_decision.result.outcome == "REJECTED_MISSING_TENANT"
    assert_tenant_enforcement_contract(
        missing_tenant_decision.envelope_metadata,
        missing_tenant_decision,
        telemetry=missing_tenant_decision.telemetry_attributes,
    )

    safe_decision = await policy.evaluate(
        sql=safe_sql,
        tenant_id=7,
        params=[],
        tenant_column="tenant_id",
        global_table_allowlist=set(),
        schema_snapshot_loader=None,
    )
    assert safe_decision.result.outcome == _safe_outcome_for_mode(mode)
    assert_tenant_enforcement_contract(
        safe_decision.envelope_metadata,
        safe_decision,
        telemetry=safe_decision.telemetry_attributes,
    )

    if mode == "sql_rewrite":
        unsupported_decision = await policy.evaluate(
            sql=example_sql["unsupported_correlated"],
            tenant_id=7,
            params=[],
            tenant_column="tenant_id",
            global_table_allowlist=set(),
            schema_snapshot_loader=None,
        )
        assert unsupported_decision.result.outcome == "REJECTED_UNSUPPORTED"
        assert_tenant_enforcement_contract(
            unsupported_decision.envelope_metadata,
            unsupported_decision,
            telemetry=unsupported_decision.telemetry_attributes,
        )
