from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from typing import Optional

from common.config.env import get_env_bool
from dal.feature_flags import experimental_features_enabled

logger = logging.getLogger(__name__)


def maybe_classify_error(provider: str, exc: Exception) -> Optional[str]:
    """Return an error category when experimental classification is enabled."""
    if not experimental_features_enabled():
        return None
    return classify_error(provider, exc)


@dataclass(frozen=True)
class ErrorClassification:
    """Structured provider-aware error classification."""

    category: str
    provider: str
    is_retryable: bool
    retry_after_seconds: Optional[float] = None


def classify_error(provider: str, exc: Exception) -> str:
    """Classify an error into a provider-agnostic category."""
    return classify_error_info(provider, exc).category


def classify_error_info(provider: str, exc: Exception) -> ErrorClassification:
    """Classify an error into a provider-aware category with retryability."""
    message = str(exc).lower()
    class_name = exc.__class__.__name__.lower()
    module_name = exc.__class__.__module__.lower()
    provider = (provider or "unknown").lower()

    retry_after = _extract_retry_after_seconds(message)

    if isinstance(exc, TimeoutError) or _matches_any(message, ("timeout", "timed out")):
        return _classification("timeout", provider, retry_after)
    if _matches_any(
        message,
        (
            "could not connect",
            "connection refused",
            "connection reset",
            "network",
            "dns",
            "connection failed",
        ),
    ):
        return _classification("connectivity", provider, retry_after)
    if _matches_any(
        message,
        (
            "permission denied",
            "not authorized",
            "access denied",
            "unauthorized",
            "insufficient privileges",
        ),
    ):
        return _classification("auth", provider, retry_after)
    if _matches_any(
        message, ("syntax error", "sql compilation error", "parse error", "invalid query")
    ):
        return _classification("syntax", provider, retry_after)
    if _matches_any(message, ("not supported", "unsupported", "feature not supported")):
        return _classification("unsupported", provider, retry_after)

    if provider in {"postgres", "redshift", "cockroachdb"}:
        if _matches_any(message, ("deadlock detected",)):
            return _classification("deadlock", provider, retry_after)
        if _matches_any(message, ("serialization failure", "could not serialize")):
            return _classification("serialization", provider, retry_after)

    if provider == "bigquery":
        if _matches_any(message, ("quota exceeded", "rate limit", "too many requests")):
            return _classification("throttling", provider, retry_after)
        if _matches_any(message, ("resources exceeded", "resource exhausted")):
            return _classification("resource_exhausted", provider, retry_after)

    if provider == "snowflake":
        if _matches_any(message, ("warehouse is suspended", "warehouse suspended", "queued")):
            return _classification("transient", provider, retry_after)

    if provider in {"athena", "databricks"} and _matches_any(
        message, ("too many requests", "service unavailable", "temporarily unavailable")
    ):
        return _classification("transient", provider, retry_after)

    # Validation / Generic fallbacks
    if _matches_any(
        message, ("too many requests", "concurrency limit", "rate limit", "throttling")
    ):
        return _classification("throttling", provider, retry_after)

    if _matches_any(
        message,
        ("disk full", "out of memory", "resource limit", "resources exceeded", "disk is full"),
    ):
        return _classification("resource_exhausted", provider, retry_after)

    if _matches_any(message, ("query execution limit exceeded", "execution time limit")):
        return _classification("timeout", provider, retry_after)

    if module_name.startswith("asyncpg") and "syntax" in class_name:
        return _classification("syntax", provider, retry_after)
    if module_name.startswith("asyncpg") and "invalidauthorization" in class_name:
        return _classification("auth", provider, retry_after)

    if class_name in {"timeout", "timeouterror"}:
        return _classification("timeout", provider, retry_after)
    if class_name in {"connectionerror", "operationalerror"}:
        return _classification("connectivity", provider, retry_after)

    return _classification("unknown", provider, retry_after)


# Recovery hints for each error category
RECOVERY_HINTS: dict[str, str] = {
    "timeout": "Consider reducing query complexity or increasing timeout budget",
    "connectivity": "Check network configuration and database availability",
    "auth": "Verify credentials and permission grants for the requested operation",
    "syntax": "Review SQL syntax; the query may reference invalid identifiers",
    "unsupported": "This operation is not supported by the current provider",
    "deadlock": "Retry automatically; consider transaction isolation adjustments",
    "serialization": "Retry automatically; reduce concurrent transaction conflicts",
    "throttling": "Reduce request rate or wait for retry_after duration",
    "resource_exhausted": "Query exceeds resource limits; simplify or paginate",
    "transient": "Retry automatically after a short delay",
    "unknown": "Inspect error details for root cause",
}


def emit_classified_error(provider: str, operation: str, category: str, exc: Exception) -> None:
    """Emit structured telemetry for classified errors when enabled.

    Sets error.classification.* span attributes for observability dashboards.
    """
    if not get_env_bool("DAL_CLASSIFIED_ERROR_TELEMETRY", True):
        return

    recovery_hint = RECOVERY_HINTS.get(category, RECOVERY_HINTS["unknown"])
    error_info = classify_error_info(provider, exc)

    try:
        from opentelemetry import trace

        span = trace.get_current_span()
        if span and span.is_recording():
            # Set structured error.classification.* attributes
            span.set_attribute("error.classification.category", category)
            span.set_attribute("error.classification.provider", provider)
            span.set_attribute("error.classification.operation", operation)
            span.set_attribute("error.classification.is_retryable", error_info.is_retryable)
            span.set_attribute("error.classification.recovery_hint", recovery_hint)
            if error_info.retry_after_seconds is not None:
                span.set_attribute(
                    "error.classification.retry_after_seconds", error_info.retry_after_seconds
                )
            span.add_event(
                "dal.error.classified",
                {
                    "provider": provider,
                    "category": category,
                    "operation": operation,
                    "is_retryable": error_info.is_retryable,
                    "recovery_hint": recovery_hint,
                },
            )
    except Exception:
        pass

    logger.error(
        "dal_error_classified",
        extra={
            "event": "dal_error_classified",
            "provider": provider,
            "operation": operation,
            "error_category": category,
            "error_type": exc.__class__.__name__,
            "is_retryable": error_info.is_retryable,
            "recovery_hint": recovery_hint,
        },
    )


def _matches_any(text: str, fragments: tuple[str, ...]) -> bool:
    return any(fragment in text for fragment in fragments)


def _classification(
    category: str, provider: str, retry_after: Optional[float]
) -> ErrorClassification:
    retryable = category in {
        "timeout",
        "connectivity",
        "throttling",
        "resource_exhausted",
        "serialization",
        "deadlock",
        "transient",
    }
    return ErrorClassification(
        category=category,
        provider=provider,
        is_retryable=retryable,
        retry_after_seconds=retry_after,
    )


def _extract_retry_after_seconds(message: str) -> Optional[float]:
    match = re.search(r"retry after\s+(\d+(?:\.\d+)?)", message)
    if not match:
        return None
    try:
        return float(match.group(1))
    except ValueError:
        return None
