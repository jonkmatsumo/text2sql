from __future__ import annotations

import logging
from typing import Optional

from common.config.env import get_env_bool
from dal.feature_flags import experimental_features_enabled

logger = logging.getLogger(__name__)


def maybe_classify_error(provider: str, exc: Exception) -> Optional[str]:
    """Return an error category when experimental classification is enabled."""
    if not experimental_features_enabled():
        return None
    return classify_error(provider, exc)


def classify_error(provider: str, exc: Exception) -> str:
    """Classify an error into a provider-agnostic category."""
    message = str(exc).lower()
    class_name = exc.__class__.__name__.lower()
    module_name = exc.__class__.__module__.lower()
    provider = provider.lower()

    if _matches_any(
        message, ("permission denied", "not authorized", "access denied", "unauthorized")
    ):
        return "auth"
    if _matches_any(
        message,
        (
            "could not connect",
            "connection refused",
            "network",
            "dns",
            "connection failed",
        ),
    ):
        return "connectivity"
    if _matches_any(message, ("timeout", "timed out", "deadline exceeded")):
        return "timeout"
    if _matches_any(message, ("out of memory", "quota", "resource exhausted", "capacity")):
        return "resource_exhausted"
    if _matches_any(
        message, ("syntax error", "sql compilation error", "parse error", "invalid query")
    ):
        return "syntax"
    if _matches_any(message, ("not supported", "unsupported", "feature not supported")):
        return "unsupported"

    if module_name.startswith("asyncpg") and "syntax" in class_name:
        return "syntax"
    if module_name.startswith("asyncpg") and "invalidauthorization" in class_name:
        return "auth"

    if provider in {"bigquery", "snowflake", "athena", "databricks"}:
        if class_name in {"badrequest", "programmingerror"}:
            return "syntax"
        if class_name in {"forbidden", "unauthorized"}:
            return "auth"
        if class_name in {"toomanyrequests", "serviceunavailable"}:
            return "transient"

    if class_name in {"timeout", "timeouterror"}:
        return "timeout"
    if class_name in {"connectionerror", "operationalerror"}:
        return "connectivity"

    return "unknown"


def emit_classified_error(provider: str, operation: str, category: str, exc: Exception) -> None:
    """Emit structured telemetry for classified errors when enabled."""
    if not get_env_bool("DAL_CLASSIFIED_ERROR_TELEMETRY", True):
        return
    try:
        from opentelemetry import trace

        span = trace.get_current_span()
        if span and span.is_recording():
            span.add_event(
                "dal.error.classified",
                {
                    "provider": provider,
                    "category": category,
                    "operation": operation,
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
        },
    )


def _matches_any(text: str, fragments: tuple[str, ...]) -> bool:
    return any(fragment in text for fragment in fragments)
