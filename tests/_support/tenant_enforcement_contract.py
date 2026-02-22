"""Shared test assertions for tenant enforcement contract invariants."""

from __future__ import annotations

from typing import Any, Mapping

from common.models.tool_envelopes import ExecuteSQLQueryMetadata

VALID_TENANT_ENFORCEMENT_MODES = {"sql_rewrite", "rls_session", "none"}
VALID_TENANT_ENFORCEMENT_OUTCOMES = {
    "APPLIED",
    "SKIPPED_NOT_REQUIRED",
    "REJECTED_UNSUPPORTED",
    "REJECTED_DISABLED",
    "REJECTED_LIMIT",
    "REJECTED_MISSING_TENANT",
    "REJECTED_TIMEOUT",
}


def assert_tenant_enforcement_contract(
    envelope: Mapping[str, Any],
    expected_result: Mapping[str, Any] | Any,
    *,
    telemetry: Mapping[str, Any] | None = None,
) -> None:
    """Assert canonical tenant enforcement metadata/telemetry invariants."""
    metadata = envelope.get("metadata", envelope)
    if not isinstance(metadata, Mapping):
        raise AssertionError("Envelope metadata must be a mapping.")

    mode = metadata.get("tenant_enforcement_mode")
    outcome = metadata.get("tenant_rewrite_outcome")
    applied = metadata.get("tenant_enforcement_applied")
    reason_code = metadata.get("tenant_rewrite_reason_code")

    assert mode in VALID_TENANT_ENFORCEMENT_MODES
    assert outcome in VALID_TENANT_ENFORCEMENT_OUTCOMES
    assert isinstance(applied, bool)

    if outcome.startswith("REJECTED_"):
        assert isinstance(reason_code, str)
        assert reason_code == reason_code.strip().lower()
        assert " " not in reason_code
    else:
        assert reason_code is None

    expected_mode = _extract_expected(expected_result, "mode", "tenant_enforcement_mode")
    if expected_mode is not None:
        assert mode == expected_mode

    expected_outcome = _extract_expected(expected_result, "outcome", "tenant_rewrite_outcome")
    if expected_outcome is not None:
        assert outcome == expected_outcome

    expected_applied = _extract_expected(expected_result, "applied", "tenant_enforcement_applied")
    if expected_applied is not None:
        assert applied is bool(expected_applied)

    expected_bounded_reason = _extract_expected(
        expected_result, "bounded_reason_code", "tenant_rewrite_reason_code"
    )
    if expected_bounded_reason is not None:
        assert reason_code == expected_bounded_reason

    # Validate bounded contract shape through envelope model fields.
    metadata_for_validation = {
        "rows_returned": 0,
        "is_truncated": False,
        "provider": "sqlite",
        **dict(metadata),
    }
    ExecuteSQLQueryMetadata.model_validate(metadata_for_validation)

    if telemetry is None:
        return

    telemetry_mode = telemetry.get("tenant.enforcement.mode")
    telemetry_outcome = telemetry.get("tenant.enforcement.outcome")
    telemetry_applied = telemetry.get("tenant.enforcement.applied")
    telemetry_reason = telemetry.get("tenant.enforcement.reason_code")

    assert telemetry_mode == mode
    assert telemetry_outcome == outcome
    assert telemetry_applied is applied
    if outcome.startswith("REJECTED_"):
        assert telemetry_reason == reason_code
    else:
        assert telemetry_reason is None


def _extract_expected(expected: Mapping[str, Any] | Any, *keys: str) -> Any | None:
    if isinstance(expected, Mapping):
        for key in keys:
            if key in expected:
                return expected[key]
        return None

    if hasattr(expected, "result"):
        result = getattr(expected, "result")
        for key in keys:
            if key == "bounded_reason_code" and hasattr(expected, "bounded_reason_code"):
                return getattr(expected, "bounded_reason_code")
            if hasattr(result, key):
                return getattr(result, key)
        return None

    for key in keys:
        if hasattr(expected, key):
            return getattr(expected, key)
    return None
