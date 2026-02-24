"""Unit tests for execution-boundary resource containment helpers."""

import json

import pytest

from dal.resource_containment import (
    ResourceContainmentPolicyError,
    enforce_byte_limit,
    enforce_row_limit,
    validate_resource_capabilities,
)


def _json_size(payload) -> int:
    return len(json.dumps(payload, default=str, separators=(",", ":")).encode("utf-8"))


def test_enforce_row_limit_truncates_when_rows_exceed_cap():
    """Row cap should truncate oversized result sets at the boundary."""
    rows = [{"id": idx} for idx in range(6)]

    bounded = enforce_row_limit(rows, max_rows=3, enforce=True)

    assert bounded.partial is True
    assert bounded.partial_reason == "max_rows"
    assert bounded.items_returned == 3
    assert bounded.rows == [{"id": 0}, {"id": 1}, {"id": 2}]


def test_enforce_row_limit_does_not_truncate_when_rows_within_cap():
    """Rows at or below cap should pass through without truncation."""
    rows = [{"id": idx} for idx in range(3)]

    bounded = enforce_row_limit(rows, max_rows=3, enforce=True)

    assert bounded.partial is False
    assert bounded.partial_reason is None
    assert bounded.items_returned == 3
    assert bounded.rows == rows


def test_enforce_row_limit_preserves_deterministic_order():
    """Capping should keep original row ordering deterministic."""
    rows = [{"id": 9}, {"id": 2}, {"id": 7}, {"id": 4}]

    bounded = enforce_row_limit(rows, max_rows=2, enforce=True)

    assert bounded.rows == [{"id": 9}, {"id": 2}]


def test_enforce_byte_limit_truncates_oversized_rows():
    """Byte cap should truncate when the first row exceeds the configured budget."""
    rows = [{"blob": "x" * 64}]
    row_size = _json_size(rows[0])

    bounded = enforce_byte_limit(rows, max_bytes=row_size - 1, enforce=True)

    assert bounded.partial is True
    assert bounded.partial_reason == "max_bytes"
    assert bounded.items_returned == 0
    assert bounded.rows == []


def test_enforce_byte_limit_handles_mixed_row_sizes_deterministically():
    """Byte cap should retain rows in order up to the last fully fitting row."""
    rows = [{"id": 1}, {"blob": "x" * 24}, {"id": 3}]
    cap = _json_size({}) + _json_size(rows[0]) + _json_size(rows[1])

    bounded = enforce_byte_limit(rows, max_bytes=cap, enforce=True)

    assert bounded.partial is True
    assert bounded.rows == [{"id": 1}, {"blob": "x" * 24}]
    assert bounded.items_returned == 2


def test_enforce_byte_limit_tracks_bytes_with_envelope_overhead():
    """Byte accounting should include envelope overhead when provided."""
    rows = [{"id": 1}]
    overhead = {"metadata": {}, "rows": []}

    bounded = enforce_byte_limit(rows, max_bytes=10_000, enforce=True, envelope_overhead=overhead)

    assert bounded.partial is False
    assert bounded.bytes_returned >= _json_size(overhead) + _json_size(rows[0])


def test_validate_resource_capabilities_accepts_supported_provider():
    """Capability validation should no-op when provider supports all requested controls."""
    validate_resource_capabilities(
        provider="postgres",
        enforce_row_limit=True,
        enforce_byte_limit=True,
        enforce_timeout=True,
        supports_row_cap=True,
        supports_byte_cap=True,
        supports_timeout=True,
    )


@pytest.mark.parametrize(
    "kwargs, expected_reason",
    [
        (
            dict(
                enforce_row_limit=True,
                enforce_byte_limit=False,
                enforce_timeout=False,
                supports_row_cap=False,
                supports_byte_cap=True,
                supports_timeout=True,
            ),
            "execution_resource_row_cap_unsupported_provider",
        ),
        (
            dict(
                enforce_row_limit=False,
                enforce_byte_limit=True,
                enforce_timeout=False,
                supports_row_cap=True,
                supports_byte_cap=False,
                supports_timeout=True,
            ),
            "execution_resource_byte_cap_unsupported_provider",
        ),
        (
            dict(
                enforce_row_limit=False,
                enforce_byte_limit=False,
                enforce_timeout=True,
                supports_row_cap=True,
                supports_byte_cap=True,
                supports_timeout=False,
            ),
            "execution_resource_timeout_unsupported_provider",
        ),
    ],
)
def test_validate_resource_capabilities_fails_closed(kwargs, expected_reason):
    """Capability mismatches should fail closed with deterministic reason codes."""
    with pytest.raises(ResourceContainmentPolicyError) as exc_info:
        validate_resource_capabilities(provider="unknown-provider", **kwargs)
    assert exc_info.value.reason_code == expected_reason
