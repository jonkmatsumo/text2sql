import time
from unittest.mock import patch

import pytest
from otel_worker.config import settings
from otel_worker.ingestion.limiter import TokenBucketLimiter


@pytest.fixture
def limiter():
    """Create a fresh limiter for each test."""
    return TokenBucketLimiter()


def test_disabled_by_default(limiter):
    """Test that limiter allows everything if disabled."""
    settings.ENABLE_RATE_LIMITING = False
    # Should acquire infinitely
    for _ in range(100):
        assert limiter.acquire() is True


def test_basic_limiting(limiter):
    """Test basic RPS enforcement."""
    settings.ENABLE_RATE_LIMITING = True
    settings.RATE_LIMIT_RPS = 10
    settings.RATE_LIMIT_BURST = 10

    # Reset limiter state manually to pick up new config
    limiter._tokens = 10
    limiter._last_update = time.monotonic()

    # Consume all tokens
    for _ in range(10):
        assert limiter.acquire() is True

    # Should be empty
    assert limiter.acquire() is False


def test_refill_logic(limiter):
    """Test that tokens refill over time."""
    settings.ENABLE_RATE_LIMITING = True
    settings.RATE_LIMIT_RPS = 10  # 1 token per 0.1s
    settings.RATE_LIMIT_BURST = 10

    limiter._tokens = 0
    limiter._last_update = time.monotonic()

    # Simulate waiting 0.2s -> +2 tokens
    with patch("time.monotonic", return_value=limiter._last_update + 0.25):
        assert limiter.acquire() is True  # 1
        assert limiter.acquire() is True  # 2
        assert limiter.acquire() is False  # 3 (exhausted)


def test_burst_cap(limiter):
    """Test that tokens do not exceed burst limit."""
    settings.ENABLE_RATE_LIMITING = True
    settings.RATE_LIMIT_RPS = 10
    settings.RATE_LIMIT_BURST = 5

    limiter._tokens = 0
    limiter._last_update = time.monotonic()

    # Simulate waiting 10s (would generate 100 tokens, but capped at 5)
    with patch("time.monotonic", return_value=limiter._last_update + 10):
        # We start with burst=5
        for _ in range(5):
            assert limiter.acquire() is True
        assert limiter.acquire() is False
