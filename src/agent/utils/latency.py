"""Helpers for latency tracking."""

from __future__ import annotations


def update_latency_ema(prev: float | None, observed: float | None, alpha: float) -> float | None:
    """Update EMA with a new observation."""
    if observed is None:
        return prev
    if prev is None:
        return observed
    alpha = max(0.0, min(1.0, float(alpha)))
    return alpha * float(observed) + (1.0 - alpha) * float(prev)
