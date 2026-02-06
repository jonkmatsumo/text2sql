"""Tests for capability negotiation fallback decisions."""

from dal.capability_negotiation import (
    CapabilityFallbackPolicy,
    negotiate_capability_request,
    parse_capability_fallback_policy,
)


def test_parse_capability_fallback_policy_defaults_to_off():
    """Unknown values should fail closed to OFF."""
    assert parse_capability_fallback_policy(None) == CapabilityFallbackPolicy.OFF
    assert parse_capability_fallback_policy("invalid") == CapabilityFallbackPolicy.OFF


def test_parse_capability_fallback_policy_accepts_known_values():
    """Known policy values should parse deterministically."""
    assert parse_capability_fallback_policy("off") == CapabilityFallbackPolicy.OFF
    assert parse_capability_fallback_policy("suggest") == CapabilityFallbackPolicy.SUGGEST
    assert parse_capability_fallback_policy("apply") == CapabilityFallbackPolicy.APPLY


def test_negotiation_keeps_supported_capability_unchanged():
    """Supported capabilities should not trigger fallback behavior."""
    result = negotiate_capability_request(
        capability_required="pagination",
        capability_supported=True,
        fallback_policy=CapabilityFallbackPolicy.APPLY,
        include_columns=True,
        timeout_seconds=None,
        page_token="tok",
        page_size=25,
    )

    assert result.capability_required is None
    assert result.capability_supported is True
    assert result.fallback_applied is False
    assert result.fallback_mode == "none"
    assert result.page_token == "tok"
    assert result.page_size == 25


def test_negotiation_off_never_applies_fallback():
    """OFF mode should fail fast without changing request semantics."""
    result = negotiate_capability_request(
        capability_required="column_metadata",
        capability_supported=False,
        fallback_policy=CapabilityFallbackPolicy.OFF,
        include_columns=True,
        timeout_seconds=None,
        page_token=None,
        page_size=None,
    )

    assert result.capability_required == "column_metadata"
    assert result.capability_supported is False
    assert result.fallback_applied is False
    assert result.fallback_mode == "disable_column_metadata"
    assert result.include_columns is True


def test_negotiation_suggest_never_applies_fallback():
    """SUGGEST mode should disclose fallback mode but avoid behavior change."""
    result = negotiate_capability_request(
        capability_required="pagination",
        capability_supported=False,
        fallback_policy=CapabilityFallbackPolicy.SUGGEST,
        include_columns=True,
        timeout_seconds=None,
        page_token="tok",
        page_size=20,
    )

    assert result.capability_required == "pagination"
    assert result.capability_supported is False
    assert result.fallback_applied is False
    assert result.fallback_mode == "force_limited_results"
    assert result.page_token == "tok"
    assert result.page_size == 20


def test_negotiation_apply_disables_column_metadata():
    """APPLY mode may disable optional column metadata capability."""
    result = negotiate_capability_request(
        capability_required="column_metadata",
        capability_supported=False,
        fallback_policy=CapabilityFallbackPolicy.APPLY,
        include_columns=True,
        timeout_seconds=None,
        page_token=None,
        page_size=None,
    )

    assert result.capability_supported is False
    assert result.fallback_applied is True
    assert result.fallback_mode == "disable_column_metadata"
    assert result.include_columns is False


def test_negotiation_apply_forces_limited_results_for_pagination():
    """APPLY mode should convert unsupported pagination into explicit limited results."""
    result = negotiate_capability_request(
        capability_required="pagination",
        capability_supported=False,
        fallback_policy=CapabilityFallbackPolicy.APPLY,
        include_columns=True,
        timeout_seconds=None,
        page_token="tok",
        page_size=50,
    )

    assert result.capability_supported is False
    assert result.fallback_applied is True
    assert result.fallback_mode == "force_limited_results"
    assert result.page_token is None
    assert result.page_size is None
    assert result.force_result_limit == 50


def test_negotiation_apply_keeps_unsupported_when_no_safe_fallback():
    """Capabilities without safe fallback should remain unsupported."""
    result = negotiate_capability_request(
        capability_required="async_cancel",
        capability_supported=False,
        fallback_policy=CapabilityFallbackPolicy.APPLY,
        include_columns=True,
        timeout_seconds=5.0,
        page_token=None,
        page_size=None,
    )

    assert result.capability_supported is False
    assert result.fallback_applied is False
    assert result.fallback_mode == "none"
    assert result.timeout_seconds == 5.0
