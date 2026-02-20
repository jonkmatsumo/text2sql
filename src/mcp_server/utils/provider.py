"""Provider-resolution helpers for tool envelopes and error metadata."""

from __future__ import annotations

from typing import Optional

_UNSPECIFIED_PROVIDER = "unspecified"


def resolve_provider(provider: Optional[str] = None) -> str:
    """Resolve a provider name with a bounded fallback.

    Priority:
    1. Explicit non-empty provider (except legacy "unknown")
    2. Active query-target provider from DAL runtime configuration
    3. "unspecified"
    """
    normalized_explicit = _normalize_provider(provider)
    if normalized_explicit is not None:
        return normalized_explicit

    try:
        from dal.database import Database

        return _normalize_provider(Database.get_query_target_provider()) or _UNSPECIFIED_PROVIDER
    except Exception:
        return _UNSPECIFIED_PROVIDER


def _normalize_provider(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    normalized = str(value).strip()
    if not normalized:
        return None
    if normalized.lower() == "unknown":
        return None
    return normalized
