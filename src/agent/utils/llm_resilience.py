"""Global LLM resilience controls for concurrency limiting and circuit breaking."""

from __future__ import annotations

import threading
import time
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


class LLMCircuitOpenError(RuntimeError):
    """Raised when the LLM circuit breaker is open."""

    category = "limit_exceeded"
    is_retryable = True

    def __init__(self, retry_after_seconds: float, consecutive_failures: int) -> None:
        """Initialize typed circuit-open error details."""
        self.retry_after_seconds = float(retry_after_seconds)
        self.consecutive_failures = int(consecutive_failures)
        super().__init__("LLM circuit breaker is open.")


class LLMGlobalConcurrencyLimiter:
    """Process-wide limiter for concurrent LLM calls and outage containment."""

    def __init__(
        self,
        *,
        max_concurrent_calls: int,
        retry_after_seconds: float,
        circuit_failure_threshold: int,
        circuit_cooldown_seconds: float,
        warm_start_cooldown_seconds: float,
        warm_start_max_concurrent_calls: int,
    ) -> None:
        """Initialize limiter and circuit-breaker state."""
        self._limit = max(1, int(max_concurrent_calls))
        self._retry_after_seconds = max(0.1, float(retry_after_seconds))
        self._circuit_failure_threshold = max(1, int(circuit_failure_threshold))
        self._circuit_cooldown_seconds = max(1.0, float(circuit_cooldown_seconds))
        self._warm_start_cooldown_seconds = max(0.0, float(warm_start_cooldown_seconds))
        self._warm_start_limit = max(
            1,
            min(int(self._limit), int(warm_start_max_concurrent_calls)),
        )
        self._created_at_monotonic = time.monotonic()
        self._active_calls = 0
        self._consecutive_failures = 0
        self._circuit_open_until_monotonic = 0.0
        self._lock = threading.Lock()

    def _warm_start_active(self, now: float) -> bool:
        if self._warm_start_cooldown_seconds <= 0:
            return False
        return (now - self._created_at_monotonic) < self._warm_start_cooldown_seconds

    def _effective_limit(self, now: float) -> int:
        if self._warm_start_active(now):
            return int(self._warm_start_limit)
        return int(self._limit)

    def _record_telemetry(self, *, active_calls: int, rate_limited: bool, now: float) -> None:
        span = trace.get_current_span()
        if span is None or not span.is_recording():
            return
        effective_limit = self._effective_limit(now)
        span.set_attribute("llm.global_active", int(active_calls))
        span.set_attribute("llm.global_limit", int(effective_limit))
        span.set_attribute("llm.rate_limited", bool(rate_limited))
        span.set_attribute("llm.circuit.failures", int(self._consecutive_failures))
        span.set_attribute("llm.circuit.state", self.circuit_state)
        span.set_attribute("system.warm_start.active", bool(self._warm_start_active(now)))

    @property
    def circuit_state(self) -> str:
        """Return current circuit state."""
        with self._lock:
            now = time.monotonic()
            if self._circuit_open_until_monotonic > now:
                return "open"
            return "closed"

    def _assert_circuit_closed(self) -> None:
        now = time.monotonic()
        with self._lock:
            remaining = self._circuit_open_until_monotonic - now
            failures = self._consecutive_failures
            active_calls = self._active_calls
        if remaining > 0:
            self._record_telemetry(active_calls=active_calls, rate_limited=False, now=now)
            raise LLMCircuitOpenError(
                retry_after_seconds=max(0.1, float(remaining)),
                consecutive_failures=failures,
            )

    def _try_acquire(self) -> LLMConcurrencyLease:
        self._assert_circuit_closed()
        now = time.monotonic()
        effective_limit = self._effective_limit(now)
        with self._lock:
            if self._active_calls >= effective_limit:
                active_calls = self._active_calls
                rate_limited = True
            else:
                self._active_calls += 1
                active_calls = self._active_calls
                rate_limited = False
        if rate_limited:
            self._record_telemetry(active_calls=active_calls, rate_limited=True, now=now)
            raise LLMRateLimitExceededError(
                retry_after_seconds=self._retry_after_seconds,
                active_calls=active_calls,
                limit=effective_limit,
            )
        self._record_telemetry(active_calls=active_calls, rate_limited=False, now=now)
        return LLMConcurrencyLease(active_calls=active_calls, limit=effective_limit)

    def _release(self) -> None:
        now = time.monotonic()
        with self._lock:
            self._active_calls = max(0, self._active_calls - 1)
            active_calls = self._active_calls
        self._record_telemetry(active_calls=active_calls, rate_limited=False, now=now)

    def record_success(self) -> None:
        """Reset consecutive failure counter after successful upstream call."""
        now = time.monotonic()
        with self._lock:
            self._consecutive_failures = 0
            self._circuit_open_until_monotonic = 0.0
            active_calls = self._active_calls
        self._record_telemetry(active_calls=active_calls, rate_limited=False, now=now)

    def record_failure(self, error: Exception) -> None:
        """Record an upstream failure and open circuit when threshold is reached."""
        if not _is_circuit_relevant_failure(error):
            return
        now = time.monotonic()
        with self._lock:
            self._consecutive_failures += 1
            if self._consecutive_failures >= self._circuit_failure_threshold:
                self._circuit_open_until_monotonic = now + self._circuit_cooldown_seconds
            active_calls = self._active_calls
        self._record_telemetry(active_calls=active_calls, rate_limited=False, now=now)

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
        max_concurrent_calls = _safe_env_int("LLM_MAX_CONCURRENT_CALLS", 8, minimum=1)
        _GLOBAL_LLM_LIMITER = LLMGlobalConcurrencyLimiter(
            max_concurrent_calls=max_concurrent_calls,
            retry_after_seconds=_safe_env_float("LLM_LIMIT_RETRY_AFTER_SECONDS", 1.0, minimum=0.1),
            circuit_failure_threshold=_safe_env_int(
                "LLM_CIRCUIT_BREAKER_FAILURE_THRESHOLD",
                5,
                minimum=1,
            ),
            circuit_cooldown_seconds=_safe_env_float(
                "LLM_CIRCUIT_BREAKER_COOLDOWN_SECONDS",
                30.0,
                minimum=1.0,
            ),
            warm_start_cooldown_seconds=_safe_env_float(
                "LLM_WARM_START_COOLDOWN_SECONDS",
                30.0,
                minimum=0.0,
            ),
            warm_start_max_concurrent_calls=_safe_env_int(
                "LLM_WARM_START_MAX_CONCURRENT_CALLS",
                max(1, max_concurrent_calls // 2),
                minimum=1,
            ),
        )
    return _GLOBAL_LLM_LIMITER


def reset_global_llm_limiter() -> None:
    """Reset global limiter singleton (test helper)."""
    global _GLOBAL_LLM_LIMITER
    _GLOBAL_LLM_LIMITER = None


def _is_circuit_relevant_failure(error: Exception) -> bool:
    """Count failures that indicate upstream LLM instability."""
    if isinstance(error, (TimeoutError,)):
        return True

    message = str(error).strip().lower()
    if not message:
        return False
    if any(fragment in message for fragment in ("timeout", "timed out")):
        return True
    if any(fragment in message for fragment in ("rate limit", "too many requests", "status 429")):
        return True
    if any(
        fragment in message
        for fragment in (
            "status 500",
            "status 502",
            "status 503",
            "status 504",
            "upstream 5xx",
            "service unavailable",
            "internal server error",
            "bad gateway",
            "gateway timeout",
        )
    ):
        return True
    return False
