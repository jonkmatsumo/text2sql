"""In-memory runtime metrics for lightweight operator diagnostics."""

from __future__ import annotations

import threading
from typing import Any

_LOCK = threading.Lock()
_LAST_STAGE_LATENCY_BREAKDOWN_MS: dict[str, float] = {}


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


def reset_runtime_metrics() -> None:
    """Reset runtime metrics (test utility)."""
    with _LOCK:
        _LAST_STAGE_LATENCY_BREAKDOWN_MS.clear()
