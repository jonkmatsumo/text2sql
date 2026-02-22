"""Conformance harness for tenant enforcement providers and modes."""

from __future__ import annotations

import inspect
import re

import pytest

from dal.capabilities import capabilities_for_provider
from tests._support.tenant_enforcement_contract import assert_tenant_enforcement_contract
from tests.unit.policy.provider_mode_matrix import (
    TENANT_ENFORCEMENT_PROVIDER_MODE_MATRIX,
    tenant_enforcement_provider_mode_rows,
)


def _capability_registry_provider_names() -> set[str]:
    source = inspect.getsource(capabilities_for_provider)
    return set(re.findall(r'normalized == "([a-z0-9_]+)"', source))


def _safe_outcome_for_mode(mode: str) -> str:
    normalized = (mode or "").strip().lower()
    if normalized in {"sql_rewrite", "rls_session"}:
        return "APPLIED"
    if normalized == "none":
        return "SKIPPED_NOT_REQUIRED"
    return "REJECTED_UNSUPPORTED"


@pytest.mark.asyncio
@pytest.mark.parametrize(("provider", "mode"), tenant_enforcement_provider_mode_rows())
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


def test_policy_conformance_matrix_covers_capability_registry() -> None:
    """Tenant-enforcement capability providers must be fully covered by the conformance matrix."""
    provider_names = _capability_registry_provider_names()
    assert provider_names, "Provider discovery failed for capabilities_for_provider"

    discovered: dict[str, str] = {}
    for provider in sorted(provider_names):
        caps = capabilities_for_provider(provider)
        if caps.supports_tenant_enforcement:
            discovered[provider] = caps.tenant_enforcement_mode

    assert discovered, "No tenant-enforcement providers discovered in capability registry"
    assert discovered == TENANT_ENFORCEMENT_PROVIDER_MODE_MATRIX


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("provider", "mode", "expected_reason_code", "expected_failure_category"),
    [
        (
            "sqlite",
            "mode_added_without_policy_support",
            "tenant_rewrite_tenant_mode_unsupported",
            None,
        ),
        (
            "provider_added_without_capability_mapping",
            "sql_rewrite",
            "tenant_rewrite_provider_unsupported",
            "tenant_rewrite_failure_dialect_unsupported",
        ),
    ],
)
async def test_policy_conformance_unknown_provider_mode_fails_closed(
    provider: str,
    mode: str,
    expected_reason_code: str,
    expected_failure_category: str | None,
    policy_factory,
    example_sql,
) -> None:
    """Unknown provider/mode combinations must fail closed with bounded reasons."""
    policy = policy_factory(provider=provider, mode=mode)
    decision = await policy.evaluate(
        sql=example_sql["safe_simple"],
        tenant_id=7,
        params=[],
        tenant_column="tenant_id",
        global_table_allowlist=set(),
        schema_snapshot_loader=None,
    )

    assert decision.should_execute is False
    assert decision.result.outcome == "REJECTED_UNSUPPORTED"
    assert decision.bounded_reason_code == expected_reason_code
    assert_tenant_enforcement_contract(
        decision.envelope_metadata,
        decision,
        telemetry=decision.telemetry_attributes,
    )
    assert decision.telemetry_attributes["tenant.enforcement.reason_code"] == expected_reason_code
    if expected_failure_category is None:
        assert "tenant_rewrite.failure_reason_category" not in decision.telemetry_attributes
    else:
        assert (
            decision.telemetry_attributes["tenant_rewrite.failure_reason_category"]
            == expected_failure_category
        )
