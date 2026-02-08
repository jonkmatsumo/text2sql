"""Canonical reason codes for system decisions, stops, and limits.

These codes are used in telemetry, logs, and API responses to provide
stable, machine-readable reasons for behavior.
"""

from enum import Enum


class PaginationStopReason(str, Enum):
    """Reasons why auto-pagination stopped."""

    MAX_PAGES = "max_pages"
    MAX_ROWS = "max_rows"
    NO_NEXT_PAGE = "no_next_page"
    BUDGET_EXHAUSTED = "budget_exhausted"
    FETCH_ERROR = "fetch_error"
    FETCH_EXCEPTION = "fetch_exception"
    NON_ENVELOPED_RESPONSE = "non_enveloped_response"
    EMPTY_PAGE_WITH_TOKEN = "empty_page_with_token"
    PATHOLOGICAL_EMPTY_PAGES = "pathological_empty_pages"
    TOKEN_REPEAT = "token_repeat"
    UNSUPPORTED_CAPABILITY = "unsupported_capability"
    DISABLED = "disabled"


class PrefetchSuppressionReason(str, Enum):
    """Reasons why next-page prefetch was skipped."""

    CACHE_HIT = "cache_hit"
    REPLAYED = "replayed"
    CACHE_MISS = "cache_miss"
    AUTO_PAGINATION_ACTIVE = "auto_pagination_active"
    AUTO_PAGINATION_ENABLED = "auto_pagination_enabled"
    NO_NEXT_PAGE = "no_next_page"
    NOT_CHEAP = "not_cheap"  # Too much data or latency on first page
    LOW_BUDGET = "low_budget"
    SCHEDULED = "scheduled"
    STORM_WAITERS = "storm_waiters"
    COOLDOWN_ACTIVE = "cooldown_active"
    ALREADY_CACHED = "already_cached"
    DUPLICATE_INFLIGHT = "duplicate_inflight"
    ALREADY_CACHED_OR_INFLIGHT = "already_cached_or_inflight"  # Kept for compatibility


class RetryDecisionReason(str, Enum):
    """Reasons for retry or failure decisions."""

    PROCEED_TO_CORRECTION = "PROCEED_TO_CORRECTION"
    NON_RETRYABLE_CATEGORY = "NON_RETRYABLE_CATEGORY"
    BUDGET_EXHAUSTED_RETRY_AFTER = "BUDGET_EXHAUSTED_RETRY_AFTER"
    INSUFFICIENT_BUDGET = "INSUFFICIENT_BUDGET"
    DEADLINE_EXCEEDED = "DEADLINE_EXCEEDED"
    MAX_RETRIES_REACHED = "MAX_RETRIES_REACHED"
    UNSUPPORTED_CAPABILITY = "UNSUPPORTED_CAPABILITY"


class ValidationRefusalReason(str, Enum):
    """Reasons why SQL validation refused the query."""

    JOIN_COMPLEXITY_EXCEEDED = "join_complexity_exceeded"
    FORBIDDEN_KEYWORD = "forbidden_keyword"
    POLICY_VIOLATION = "policy_violation"


class PayloadTruncationReason(str, Enum):
    """Reasons why a payload was truncated."""

    MAX_ROWS = "max_rows"  # Row count limit
    MAX_BYTES = "max_bytes"  # JSON size limit
    PROVIDER_CAP = "provider_cap"
    SAFETY_LIMIT = "safety_limit"


class DriftDetectionMethod(str, Enum):
    """Methods used for drift detection."""

    AST = "ast"
    REGEX_FALLBACK = "regex_fallback"
    HYBRID = "hybrid"
