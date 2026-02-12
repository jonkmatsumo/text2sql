"""Global LLM resilience controls for concurrency limiting."""

from __future__ import annotations

import threading
from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass
from typing import AsyncIterator, Iterator, Optional

from opentelemetry import trace

from common.config.env import get_env_float, get_env_int


@dataclass(frozen=True)
class LLMConcurrencyLease:
    """Active global LLM call lease."""

    active_calls: int
    limit: int


class LLMRateLimitExceededError(RuntimeError):
    """Raised when global LLM concurrency limits are exceeded."""

    category = "limit_exceeded"
    is_retryable = True

    def __init__(self, retry_after_seconds: float, active_calls: int, limit: int) -> None:
        """Initialize typed LLM rate-limit error details."""
        self.retry_after_seconds = float(retry_after_seconds)
        self.active_calls = int(active_calls)
        self.limit = int(limit)
        super().__init__("LLM global concurrency limit exceeded.")


class LLMGlobalConcurrencyLimiter:
    """Process-wide limiter for concurrent LLM calls."""

    def __init__(self, *, max_concurrent_calls: int, retry_after_seconds: float) -> None:
        """Initialize limiter thresholds and internal state."""
        self._limit = max(1, int(max_concurrent_calls))
        self._retry_after_seconds = max(0.1, float(retry_after_seconds))
        self._active_calls = 0
        self._lock = threading.Lock()

    def _record_telemetry(self, *, active_calls: int, rate_limited: bool) -> None:
        span = trace.get_current_span()
        if span is None or not span.is_recording():
            return
        span.set_attribute("llm.global_active", int(active_calls))
        span.set_attribute("llm.global_limit", int(self._limit))
        span.set_attribute("llm.rate_limited", bool(rate_limited))

    def _try_acquire(self) -> LLMConcurrencyLease:
        with self._lock:
            if self._active_calls >= self._limit:
                self._record_telemetry(active_calls=self._active_calls, rate_limited=True)
                raise LLMRateLimitExceededError(
                    retry_after_seconds=self._retry_after_seconds,
                    active_calls=self._active_calls,
                    limit=self._limit,
                )
            self._active_calls += 1
            active_calls = self._active_calls
        self._record_telemetry(active_calls=active_calls, rate_limited=False)
        return LLMConcurrencyLease(active_calls=active_calls, limit=self._limit)

    def _release(self) -> None:
        with self._lock:
            self._active_calls = max(0, self._active_calls - 1)
            active_calls = self._active_calls
        self._record_telemetry(active_calls=active_calls, rate_limited=False)

    @contextmanager
    def acquire_sync(self) -> Iterator[LLMConcurrencyLease]:
        """Acquire a synchronous lease or raise a typed limit exception."""
        lease = self._try_acquire()
        try:
            yield lease
        finally:
            self._release()

    @asynccontextmanager
    async def acquire_async(self) -> AsyncIterator[LLMConcurrencyLease]:
        """Acquire an async lease or raise a typed limit exception."""
        lease = self._try_acquire()
        try:
            yield lease
        finally:
            self._release()


_GLOBAL_LLM_LIMITER: Optional[LLMGlobalConcurrencyLimiter] = None


def _safe_env_int(name: str, default: int, minimum: int) -> int:
    try:
        value = get_env_int(name, default)
    except ValueError:
        value = default
    if value is None:
        value = default
    return max(minimum, int(value))


def _safe_env_float(name: str, default: float, minimum: float) -> float:
    try:
        value = get_env_float(name, default)
    except ValueError:
        value = default
    if value is None:
        value = default
    return max(minimum, float(value))


def get_global_llm_limiter() -> LLMGlobalConcurrencyLimiter:
    """Return process-wide LLM concurrency limiter singleton."""
    global _GLOBAL_LLM_LIMITER
    if _GLOBAL_LLM_LIMITER is None:
        _GLOBAL_LLM_LIMITER = LLMGlobalConcurrencyLimiter(
            max_concurrent_calls=_safe_env_int("LLM_MAX_CONCURRENT_CALLS", 8, minimum=1),
            retry_after_seconds=_safe_env_float("LLM_LIMIT_RETRY_AFTER_SECONDS", 1.0, minimum=0.1),
        )
    return _GLOBAL_LLM_LIMITER


def reset_global_llm_limiter() -> None:
    """Reset global limiter singleton (test helper)."""
    global _GLOBAL_LLM_LIMITER
    _GLOBAL_LLM_LIMITER = None
