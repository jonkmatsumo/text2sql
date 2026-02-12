"""Helpers for run-scoped schema snapshot pinning."""

from __future__ import annotations

import time
from typing import Any, Mapping


def resolve_pinned_schema_snapshot_id(state: Mapping[str, Any]) -> str:
    """Return the run-scoped pinned snapshot id, falling back to legacy state."""
    pinned = state.get("pinned_schema_snapshot_id")
    if isinstance(pinned, str) and pinned:
        return pinned

    legacy = state.get("schema_snapshot_id")
    if isinstance(legacy, str) and legacy:
        return legacy

    return "unknown"


def build_schema_snapshot_transition(
    old_snapshot_id: str | None,
    new_snapshot_id: str | None,
    *,
    reason: str,
) -> dict[str, Any] | None:
    """Build stable old→new transition metadata for schema refresh auditing."""
    old = (old_snapshot_id or "").strip() or None
    new = (new_snapshot_id or "").strip() or None
    if not old or not new or old == new:
        return None

    return {
        "old_snapshot_id": old,
        "new_snapshot_id": new,
        "reason": reason,
        "updated_at": int(time.time()),
    }


def apply_pending_schema_snapshot_refresh(
    state: Mapping[str, Any],
    *,
    candidate_snapshot_id: str | None,
    candidate_fingerprint: str | None = None,
    candidate_version_ts: int | None = None,
    reason: str = "schema_refresh",
) -> dict[str, Any]:
    """Apply a pending snapshot to run-scoped state exactly once per transition."""
    prior_snapshot_id = resolve_pinned_schema_snapshot_id(state)
    refresh_applied = int(state.get("schema_snapshot_refresh_applied") or 0)

    updated_snapshot_id = prior_snapshot_id
    transition = None
    if candidate_snapshot_id and candidate_snapshot_id != prior_snapshot_id:
        updated_snapshot_id = str(candidate_snapshot_id)
        refresh_applied += 1
        transition = build_schema_snapshot_transition(
            prior_snapshot_id,
            updated_snapshot_id,
            reason=reason,
        )

    return {
        "schema_snapshot_id": updated_snapshot_id,
        "pinned_schema_snapshot_id": updated_snapshot_id,
        "schema_fingerprint": candidate_fingerprint or state.get("schema_fingerprint"),
        "schema_version_ts": candidate_version_ts or state.get("schema_version_ts"),
        "pending_schema_snapshot_id": None,
        "pending_schema_fingerprint": None,
        "pending_schema_version_ts": None,
        "schema_snapshot_transition": transition,
        "schema_snapshot_refresh_applied": refresh_applied,
    }
