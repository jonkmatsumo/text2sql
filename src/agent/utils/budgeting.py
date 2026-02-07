"""Budgeting and latency estimation utilities."""

import time
from typing import Optional


def update_latency_ema(
    prev: float | None, observed: float | None, alpha: float, dampen: bool = False
) -> float | None:
    """Update EMA with a new observation, optionally applying dampening.

    If dampen is True, the alpha is halved to make the update more conservative
    (trusting history more than the new sample).
    """
    if observed is None:
        return prev
    if prev is None:
        return observed

    alpha = max(0.0, min(1.0, float(alpha)))

    if dampen:
        alpha = alpha * 0.5

    return alpha * float(observed) + (1.0 - alpha) * float(prev)


class Budget:
    """Helper for managing time budgets."""

    def __init__(self, timeout_seconds: float, deadline_ts: Optional[float] = None):
        """Initialize budget with timeout or explicit deadline."""
        self.timeout_seconds = float(timeout_seconds)
        if deadline_ts is not None:
            self.deadline_ts = float(deadline_ts)
        else:
            self.deadline_ts = time.monotonic() + self.timeout_seconds

    def remaining(self) -> float:
        """Get remaining time in seconds."""
        return max(0.0, self.deadline_ts - time.monotonic())

    def is_exhausted(self) -> bool:
        """Check if budget is exhausted."""
        return self.remaining() <= 0
