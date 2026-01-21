import time
from threading import Lock

from otel_worker.config import settings


class TokenBucketLimiter:
    """Thread-safe Token Bucket Rate Limiter."""

    def __init__(self):
        """Initialize the rate limiter with config settings."""
        self._tokens = float(settings.RATE_LIMIT_BURST)
        self._last_update = time.monotonic()
        self._lock = Lock()

    def acquire(self, tokens: int = 1) -> bool:
        """
        Attempt to acquire tokens.

        Returns:
            True if allowed, False if limited.
        """
        if not settings.ENABLE_RATE_LIMITING:
            return True

        with self._lock:
            current_time = time.monotonic()
            elapsed = current_time - self._last_update

            # Refill tokens based on RPS
            refill = elapsed * settings.RATE_LIMIT_RPS
            self._tokens = min(float(settings.RATE_LIMIT_BURST), self._tokens + refill)
            self._last_update = current_time

            if self._tokens >= tokens:
                self._tokens -= tokens
                return True
            return False


# Global limiter instance
limiter = TokenBucketLimiter()
