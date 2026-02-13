import random


def compute_retry_delay(
    retry_after: float, jitter_ratio: float = 0.2, max_delay: float = 60.0
) -> float:
    """Compute the actual sleep duration with jitter and bounds.

    Args:
        retry_after: The base wait time requested (seconds).
        jitter_ratio: Fraction of base time to use for jitter (default 0.2 = +/- 20%).
        max_delay: Absolute maximum wait time cap (seconds).

    Returns:
        Float seconds to sleep.
    """
    base = min(max(0.0, retry_after), max_delay)
    if base <= 0:
        return 0.0

    # Jitter: +/- jitter_ratio * base
    jitter_range = base * jitter_ratio
    jitter = random.uniform(-jitter_range, jitter_range)

    return max(0.0, base + jitter)
