"""In-memory runtime metrics for lightweight operator diagnostics."""

from __future__ import annotations

import threading
from collections import deque
from typing import Any

_LOCK = threading.Lock()
_LAST_STAGE_LATENCY_BREAKDOWN_MS: dict[str, float] = {}
_QUERY_COMPLEXITY_SCORES: deque[float] = deque(maxlen=128)
_RECENT_TRUNCATION_EVENTS: deque[bool] = deque(maxlen=128)


def _coerce_ms(value: Any) -> float:
    if isinstance(value, (int, float)):
        return max(0.0, float(value))
    return 0.0


def record_stage_latency_breakdown(latency_breakdown: dict[str, Any] | None) -> None:
    """Record the most recent stage-latency breakdown in milliseconds."""
    if not isinstance(latency_breakdown, dict):
        return
    normalized = {
        "retrieval_ms": _coerce_ms(latency_breakdown.get("retrieval_ms")),
        "planning_ms": _coerce_ms(latency_breakdown.get("planning_ms")),
        "generation_ms": _coerce_ms(latency_breakdown.get("generation_ms")),
        "validation_ms": _coerce_ms(latency_breakdown.get("validation_ms")),
        "execution_ms": _coerce_ms(latency_breakdown.get("execution_ms")),
        "correction_loop_ms": _coerce_ms(latency_breakdown.get("correction_loop_ms")),
    }
    with _LOCK:
        _LAST_STAGE_LATENCY_BREAKDOWN_MS.clear()
        _LAST_STAGE_LATENCY_BREAKDOWN_MS.update(normalized)


def get_stage_latency_breakdown() -> dict[str, float]:
    """Return the most recently recorded stage-latency breakdown."""
    with _LOCK:
        return dict(_LAST_STAGE_LATENCY_BREAKDOWN_MS)


def record_query_complexity_score(score: Any) -> None:
    """Record a query complexity score into a bounded rolling window."""
    if not isinstance(score, (int, float)):
        return
    normalized = max(0.0, float(score))
    with _LOCK:
        _QUERY_COMPLEXITY_SCORES.append(normalized)


def get_average_query_complexity() -> float:
    """Return rolling average complexity score from recent requests."""
    with _LOCK:
        if not _QUERY_COMPLEXITY_SCORES:
            return 0.0
        return float(sum(_QUERY_COMPLEXITY_SCORES) / len(_QUERY_COMPLEXITY_SCORES))


def record_truncation_event(is_truncated: Any) -> None:
    """Record whether the latest request produced a truncated result."""
    with _LOCK:
        _RECENT_TRUNCATION_EVENTS.append(bool(is_truncated))


def get_recent_truncation_event_count() -> int:
    """Return count of truncation events in the bounded recent window."""
    with _LOCK:
        return int(sum(1 for event in _RECENT_TRUNCATION_EVENTS if event))


def reset_runtime_metrics() -> None:
    """Reset runtime metrics (test utility)."""
    with _LOCK:
        _LAST_STAGE_LATENCY_BREAKDOWN_MS.clear()
        _QUERY_COMPLEXITY_SCORES.clear()
        _RECENT_TRUNCATION_EVENTS.clear()
