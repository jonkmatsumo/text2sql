"""Capability negotiation helpers for explicit fallback behavior."""

from dataclasses import dataclass
from enum import Enum
from typing import Optional


class CapabilityFallbackPolicy(str, Enum):
    """Policy for capability fallback behavior."""

    OFF = "off"
    SUGGEST = "suggest"
    APPLY = "apply"


class CapabilityFallbackMode(str, Enum):
    """Available fallback modes when capability is unsupported."""

    NONE = "none"
    DISABLE_PAGINATION = "disable_pagination"
    DISABLE_COLUMN_METADATA = "disable_column_metadata"
    FORCE_LIMITED_RESULTS = "force_limited_results"


@dataclass(frozen=True)
class CapabilityNegotiationResult:
    """Negotiation result with potentially adjusted execution options."""

    capability_required: Optional[str]
    capability_supported: bool
    fallback_applied: bool
    fallback_mode: str
    include_columns: bool
    timeout_seconds: Optional[float]
    page_token: Optional[str]
    page_size: Optional[int]
    force_result_limit: Optional[int] = None

    def to_metadata(self) -> dict:
        """Serialize result metadata for response payloads."""
        return {
            "capability_required": self.capability_required,
            "capability_supported": self.capability_supported,
            "fallback_applied": self.fallback_applied,
            "fallback_mode": self.fallback_mode,
        }


def parse_capability_fallback_policy(raw_value: Optional[str]) -> CapabilityFallbackPolicy:
    """Parse fallback policy from environment/config string."""
    normalized = (raw_value or CapabilityFallbackPolicy.OFF.value).strip().lower()
    if normalized == CapabilityFallbackPolicy.SUGGEST.value:
        return CapabilityFallbackPolicy.SUGGEST
    if normalized == CapabilityFallbackPolicy.APPLY.value:
        return CapabilityFallbackPolicy.APPLY
    return CapabilityFallbackPolicy.OFF


def _fallback_mode_for(
    capability_required: str,
    page_token: Optional[str],
    page_size: Optional[int],
) -> CapabilityFallbackMode:
    if capability_required == "column_metadata":
        return CapabilityFallbackMode.DISABLE_COLUMN_METADATA
    if capability_required == "pagination":
        if page_size is not None and page_size > 0:
            return CapabilityFallbackMode.FORCE_LIMITED_RESULTS
        if page_token:
            return CapabilityFallbackMode.DISABLE_PAGINATION
        return CapabilityFallbackMode.FORCE_LIMITED_RESULTS
    return CapabilityFallbackMode.NONE


def negotiate_capability_request(
    *,
    capability_required: str,
    capability_supported: bool,
    fallback_policy: CapabilityFallbackPolicy,
    include_columns: bool,
    timeout_seconds: Optional[float],
    page_token: Optional[str],
    page_size: Optional[int],
) -> CapabilityNegotiationResult:
    """Negotiate an explicit fallback when required capability is unsupported."""
    if capability_supported:
        return CapabilityNegotiationResult(
            capability_required=None,
            capability_supported=True,
            fallback_applied=False,
            fallback_mode=CapabilityFallbackMode.NONE.value,
            include_columns=include_columns,
            timeout_seconds=timeout_seconds,
            page_token=page_token,
            page_size=page_size,
            force_result_limit=None,
        )

    fallback_mode = _fallback_mode_for(capability_required, page_token, page_size)
    if (
        fallback_policy != CapabilityFallbackPolicy.APPLY
        or fallback_mode == CapabilityFallbackMode.NONE
    ):
        return CapabilityNegotiationResult(
            capability_required=capability_required,
            capability_supported=False,
            fallback_applied=False,
            fallback_mode=fallback_mode.value,
            include_columns=include_columns,
            timeout_seconds=timeout_seconds,
            page_token=page_token,
            page_size=page_size,
            force_result_limit=None,
        )

    adjusted_include_columns = include_columns
    adjusted_timeout = timeout_seconds
    adjusted_page_token = page_token
    adjusted_page_size = page_size
    force_result_limit = None

    if fallback_mode == CapabilityFallbackMode.DISABLE_COLUMN_METADATA:
        adjusted_include_columns = False
    elif fallback_mode == CapabilityFallbackMode.DISABLE_PAGINATION:
        adjusted_page_token = None
        adjusted_page_size = None
    elif fallback_mode == CapabilityFallbackMode.FORCE_LIMITED_RESULTS:
        force_result_limit = page_size if page_size is not None and page_size > 0 else None
        adjusted_page_token = None
        adjusted_page_size = None

    return CapabilityNegotiationResult(
        capability_required=capability_required,
        capability_supported=False,
        fallback_applied=True,
        fallback_mode=fallback_mode.value,
        include_columns=adjusted_include_columns,
        timeout_seconds=adjusted_timeout,
        page_token=adjusted_page_token,
        page_size=adjusted_page_size,
        force_result_limit=force_result_limit,
    )
