"""Drift guards for policy mode/provider coverage and result invariants."""

import inspect
import re

import pytest

from common.models.tool_envelopes import ExecuteSQLQueryMetadata
from common.security.tenant_enforcement_policy import TenantEnforcementPolicy
from dal.capabilities import capabilities_for_provider

_VALID_MODES = {"sql_rewrite", "rls_session", "none"}
_VALID_OUTCOMES = {
    "APPLIED",
    "SKIPPED_NOT_REQUIRED",
    "REJECTED_UNSUPPORTED",
    "REJECTED_DISABLED",
    "REJECTED_LIMIT",
    "REJECTED_MISSING_TENANT",
    "REJECTED_TIMEOUT",
}


def _policy(provider: str, mode: str) -> TenantEnforcementPolicy:
    return TenantEnforcementPolicy(
        provider=provider,
        mode=mode,
        strict=True,
        max_targets=25,
        max_params=50,
        max_ast_nodes=1000,
        hard_timeout_ms=200,
        warn_ms=50,
    )


def _assert_decision_invariants(decision) -> None:
    assert decision.result.mode
    assert decision.result.outcome in _VALID_OUTCOMES
    assert isinstance(decision.result.applied, bool)

    if decision.result.outcome.startswith("REJECTED_"):
        assert decision.result.reason_code is not None
    else:
        assert decision.result.reason_code is None

    metadata = decision.envelope_metadata
    assert metadata["tenant_enforcement_mode"] in _VALID_MODES
    assert metadata["tenant_rewrite_outcome"] == decision.result.outcome
    assert metadata["tenant_enforcement_applied"] is decision.result.applied

    if decision.result.outcome.startswith("REJECTED_"):
        reason_code = metadata.get("tenant_rewrite_reason_code")
        assert isinstance(reason_code, str)
        assert reason_code == decision.bounded_reason_code
        assert reason_code == reason_code.strip().lower()
        assert " " not in reason_code
    else:
        assert metadata.get("tenant_rewrite_reason_code") is None

    telemetry = decision.telemetry_attributes
    assert telemetry["tenant.enforcement.mode"] == metadata["tenant_enforcement_mode"]
    assert telemetry["tenant.enforcement.outcome"] == metadata["tenant_rewrite_outcome"]
    assert telemetry["tenant.enforcement.applied"] is metadata["tenant_enforcement_applied"]
    if decision.result.outcome.startswith("REJECTED_"):
        telemetry_reason = telemetry.get("tenant.enforcement.reason_code")
        assert isinstance(telemetry_reason, str)
        assert telemetry_reason == decision.bounded_reason_code

    validated = ExecuteSQLQueryMetadata(
        rows_returned=0,
        is_truncated=False,
        provider="sqlite",
        **metadata,
    )
    assert validated.tenant_enforcement_mode == metadata["tenant_enforcement_mode"]
    assert validated.tenant_rewrite_outcome == metadata["tenant_rewrite_outcome"]
    assert validated.tenant_enforcement_applied == metadata["tenant_enforcement_applied"]


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
    provider: str, mode: str, sql: str, tenant_id: int | None
):
    """Policy decision payload should preserve invariant envelope and telemetry contracts."""
    decision = await _policy(provider, mode).evaluate(
        sql=sql,
        tenant_id=tenant_id,
        params=[],
        tenant_column="tenant_id",
        global_table_allowlist=set(),
        schema_snapshot_loader=None,
    )
    _assert_decision_invariants(decision)


def test_policy_drift_guard_provider_capability_coverage():
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

        decision = _policy(provider, caps.tenant_enforcement_mode).default_decision(
            sql="SELECT 1",
            params=[],
        )
        assert decision.envelope_metadata["tenant_enforcement_mode"] in _VALID_MODES
        assert decision.envelope_metadata["tenant_rewrite_outcome"] in _VALID_OUTCOMES
        assert isinstance(decision.envelope_metadata["tenant_enforcement_applied"], bool)

    assert covered, "No tenant-enforcement providers discovered in capabilities registry"
