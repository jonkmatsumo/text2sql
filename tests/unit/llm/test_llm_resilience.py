"""Tests for global LLM limiter and circuit breaker behavior."""

import asyncio
import time
from unittest.mock import MagicMock

import pytest

from agent.llm_client import _wrap_llm
from agent.utils.llm_resilience import (
    LLMCircuitOpenError,
    LLMRateLimitExceededError,
    get_global_llm_limiter,
    reset_global_llm_limiter,
)


@pytest.fixture(autouse=True)
def _reset_limiter():
    reset_global_llm_limiter()
    yield
    reset_global_llm_limiter()


@pytest.mark.asyncio
async def test_global_limiter_rejects_when_concurrency_exceeded(monkeypatch):
    """Second in-flight call should fail fast with typed limit-exceeded error."""
    monkeypatch.setenv("LLM_MAX_CONCURRENT_CALLS", "1")
    monkeypatch.setenv("LLM_LIMIT_RETRY_AFTER_SECONDS", "2.5")
    limiter = get_global_llm_limiter()
    release = asyncio.Event()

    async def _hold_slot() -> None:
        async with limiter.acquire_async():
            await release.wait()

    task = asyncio.create_task(_hold_slot())
    await asyncio.sleep(0)

    try:
        with pytest.raises(LLMRateLimitExceededError) as exc_info:
            async with limiter.acquire_async():
                pass
        assert exc_info.value.category == "limit_exceeded"
        assert exc_info.value.retry_after_seconds == 2.5
    finally:
        release.set()
        await task


def test_circuit_opens_after_threshold_failures(monkeypatch):
    """Consecutive upstream failures should open the circuit."""
    monkeypatch.setenv("LLM_CIRCUIT_BREAKER_FAILURE_THRESHOLD", "2")
    monkeypatch.setenv("LLM_CIRCUIT_BREAKER_COOLDOWN_SECONDS", "30")
    limiter = get_global_llm_limiter()

    limiter.record_failure(TimeoutError("upstream timed out"))
    limiter.record_failure(RuntimeError("status 503 service unavailable"))

    assert limiter.circuit_state == "open"
    with pytest.raises(LLMCircuitOpenError) as exc_info:
        with limiter.acquire_sync():
            pass
    assert exc_info.value.category == "limit_exceeded"
    assert exc_info.value.consecutive_failures == 2
    assert exc_info.value.retry_after_seconds > 0


def test_circuit_resets_on_success(monkeypatch):
    """A successful call should close the circuit and clear failure streak."""
    monkeypatch.setenv("LLM_CIRCUIT_BREAKER_FAILURE_THRESHOLD", "1")
    monkeypatch.setenv("LLM_CIRCUIT_BREAKER_COOLDOWN_SECONDS", "30")
    limiter = get_global_llm_limiter()

    limiter.record_failure(TimeoutError("timeout"))
    assert limiter.circuit_state == "open"

    limiter.record_success()
    assert limiter.circuit_state == "closed"
    with limiter.acquire_sync():
        pass


def test_wrapper_raises_limit_exceeded_when_limiter_is_full(monkeypatch):
    """Wrapper should propagate typed global limiter errors."""
    monkeypatch.setenv("LLM_MAX_CONCURRENT_CALLS", "1")
    monkeypatch.setenv("LLM_LIMIT_RETRY_AFTER_SECONDS", "1.25")
    limiter = get_global_llm_limiter()

    mock_llm = MagicMock()
    mock_llm.model_name = "test-model"
    mock_response = MagicMock()
    mock_response.content = "ok"
    mock_llm.invoke.return_value = mock_response
    wrapped = _wrap_llm(mock_llm)

    with limiter.acquire_sync():
        with pytest.raises(LLMRateLimitExceededError) as exc_info:
            wrapped.invoke("hello")
    assert exc_info.value.retry_after_seconds == 1.25


def test_wrapper_opens_circuit_after_upstream_failure(monkeypatch):
    """Wrapper should open circuit after repeated upstream failure patterns."""
    monkeypatch.setenv("LLM_CIRCUIT_BREAKER_FAILURE_THRESHOLD", "1")
    monkeypatch.setenv("LLM_CIRCUIT_BREAKER_COOLDOWN_SECONDS", "10")
    limiter = get_global_llm_limiter()

    mock_llm = MagicMock()
    mock_llm.model_name = "test-model"
    mock_llm.invoke.side_effect = TimeoutError("upstream timeout")
    wrapped = _wrap_llm(mock_llm)

    with pytest.raises(TimeoutError):
        wrapped.invoke("first")

    assert limiter.circuit_state == "open"
    with pytest.raises(LLMCircuitOpenError):
        wrapped.invoke("second")


def test_circuit_cooldown_expires(monkeypatch):
    """Circuit should close automatically after cooldown elapses."""
    monkeypatch.setenv("LLM_CIRCUIT_BREAKER_FAILURE_THRESHOLD", "1")
    monkeypatch.setenv("LLM_CIRCUIT_BREAKER_COOLDOWN_SECONDS", "1")
    limiter = get_global_llm_limiter()

    limiter.record_failure(TimeoutError("upstream timeout"))
    assert limiter.circuit_state == "open"

    time.sleep(1.05)
    assert limiter.circuit_state == "closed"
