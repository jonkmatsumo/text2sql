"""Structured decision-event helpers for retry/routing diagnostics."""

from __future__ import annotations

import json
import time
from dataclasses import asdict, dataclass
from typing import Any, Optional

from common.config.env import get_env_int


@dataclass(frozen=True)
class DecisionEvent:
    """Stable schema for decision diagnostics."""

    run_id: str
    node: str
    decision: str
    reason: str
    retry_count: int
    error_category: Optional[str]
    retry_after_seconds: Optional[float]
    timestamp: float

    def to_dict(self) -> dict[str, Any]:
        """Convert event to dictionary, omitting null optional fields."""
        payload = asdict(self)
        if payload.get("error_category") is None:
            payload.pop("error_category", None)
        if payload.get("retry_after_seconds") is None:
            payload.pop("retry_after_seconds", None)
        return payload


def _safe_env_int(name: str, default: int, minimum: int) -> int:
    try:
        value = get_env_int(name, default)
    except ValueError:
        value = default
    if value is None:
        value = default
    return max(minimum, int(value))


def append_decision_event(
    state: dict[str, Any],
    *,
    node: str,
    decision: str,
    reason: str,
    retry_count: int,
    error_category: Optional[str] = None,
    retry_after_seconds: Optional[float] = None,
    span: Optional[Any] = None,
) -> dict[str, Any]:
    """Append a bounded structured decision event and emit it to span events."""
    max_events = _safe_env_int("AGENT_DECISION_EVENTS_MAX_EVENTS", 40, minimum=1)
    max_chars = _safe_env_int("AGENT_DECISION_EVENTS_MAX_CHARS", 12000, minimum=512)

    run_id = str(state.get("run_id") or "unknown")
    event = DecisionEvent(
        run_id=run_id,
        node=str(node),
        decision=str(decision),
        reason=str(reason),
        retry_count=int(retry_count),
        error_category=(str(error_category) if error_category else None),
        retry_after_seconds=(
            float(retry_after_seconds) if retry_after_seconds is not None else None
        ),
        timestamp=float(time.time()),
    ).to_dict()

    existing_events_raw = state.get("decision_events") or []
    existing_events = [entry for entry in existing_events_raw if isinstance(entry, dict)]
    existing_truncated = bool(state.get("decision_events_truncated"))
    existing_dropped = int(state.get("decision_events_dropped", 0) or 0)

    events = existing_events + [event]
    dropped_now = 0
    if len(events) > max_events:
        dropped_now = len(events) - max_events
        events = events[dropped_now:]

    while events and len(json.dumps(events, default=str)) > max_chars:
        events.pop(0)
        dropped_now += 1

    state["decision_events"] = events
    state["decision_events_truncated"] = bool(existing_truncated or dropped_now > 0)
    state["decision_events_dropped"] = int(existing_dropped + dropped_now)

    if span is not None:
        span.add_event("agent.decision_event", event)

    return event


def summarize_decision_events(state: dict[str, Any]) -> dict[str, int]:
    """Summarize event counts by decision type."""
    counts: dict[str, int] = {}
    events = state.get("decision_events") or []
    if not isinstance(events, list):
        return counts
    for event in events:
        if not isinstance(event, dict):
            continue
        decision = event.get("decision")
        if not isinstance(decision, str) or not decision:
            continue
        counts[decision] = int(counts.get(decision, 0)) + 1
    return {key: counts[key] for key in sorted(counts.keys())}
