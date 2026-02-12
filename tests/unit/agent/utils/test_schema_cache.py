"""Tests for schema snapshot cache."""

import asyncio
from unittest.mock import patch

import pytest

from agent.utils.schema_cache import (
    MemorySchemaSnapshotCache,
    SchemaRefreshLimitExceeded,
    get_cached_schema_snapshot_id,
    get_or_refresh_schema_snapshot_id,
    get_schema_cache,
    get_schema_refresh_collision_count,
    reset_schema_cache,
    set_cached_schema_snapshot_id,
)


@pytest.mark.asyncio
async def test_memory_cache_set_get():
    """Test basic set and get operations."""
    cache = MemorySchemaSnapshotCache()
    key = "tmptable"
    val = {"columns": ["a", "b"]}

    await cache.set(key, val)
    cached = await cache.get(key)

    assert cached == val


@pytest.mark.asyncio
async def test_memory_cache_expiry():
    """Test standard expiration."""
    cache = MemorySchemaSnapshotCache()
    key = "expired"
    val = {"foo": "bar"}

    # We can't mock time.monotonic easily without patching time module globally or injecting clock.
    # For now, we trust logic if logic is correct, or patch time.monotonic.

    # Trust logic by setting negative TTL
    # no-op block to replace invalid code

    # Actually let's just test logic correctness by inspecting underlying dict or standard logic
    await cache.set(key, val, ttl_seconds=-1)  # Already expired
    assert await cache.get(key) is None


@pytest.mark.asyncio
async def test_factory_singleton():
    """Test factory returns singleton."""
    c1 = get_schema_cache()
    c2 = get_schema_cache()
    assert c1 is c2
    assert isinstance(c1, MemorySchemaSnapshotCache)


def test_schema_snapshot_id_ttl_expiry_and_refresh(monkeypatch):
    """Snapshot IDs should expire after TTL and accept refreshed values."""
    monkeypatch.setenv("SCHEMA_CACHE_TTL_SECONDS", "1")

    set_cached_schema_snapshot_id(tenant_id=42, snapshot_id="fp-old", now=100.0)

    assert get_cached_schema_snapshot_id(tenant_id=42, now=100.5) == "fp-old"
    assert get_cached_schema_snapshot_id(tenant_id=42, now=101.0) is None

    set_cached_schema_snapshot_id(tenant_id=42, snapshot_id="fp-new", now=101.1)
    assert get_cached_schema_snapshot_id(tenant_id=42, now=101.2) == "fp-new"


def test_schema_snapshot_id_ignores_stale_race_writes():
    """Older racing writes should not overwrite a newer snapshot."""
    set_cached_schema_snapshot_id(tenant_id=9, snapshot_id="fp-old", now=200.0)
    set_cached_schema_snapshot_id(tenant_id=9, snapshot_id="fp-new", now=201.0)
    set_cached_schema_snapshot_id(tenant_id=9, snapshot_id="fp-stale", now=200.5)

    assert get_cached_schema_snapshot_id(tenant_id=9, now=201.1) == "fp-new"


@pytest.mark.asyncio
async def test_schema_snapshot_refresh_single_flight_prevents_duplicate_refreshes():
    """Concurrent refreshes for one tenant should execute refresh_fn once."""
    reset_schema_cache()
    calls = {"count": 0}

    async def refresh_fn():
        calls["count"] += 1
        await asyncio.sleep(0.01)
        return "fp-concurrent"

    results = await asyncio.gather(
        get_or_refresh_schema_snapshot_id(tenant_id=77, refresh_fn=refresh_fn),
        get_or_refresh_schema_snapshot_id(tenant_id=77, refresh_fn=refresh_fn),
    )

    assert results == ["fp-concurrent", "fp-concurrent"]
    assert calls["count"] == 1
    assert get_schema_refresh_collision_count() >= 1


def test_schema_cache_lru_evicts_oldest_tenant(monkeypatch):
    """LRU eviction should drop the least recently used tenant snapshot."""
    reset_schema_cache()
    monkeypatch.setenv("SCHEMA_CACHE_MAX_ENTRIES", "2")
    monkeypatch.setenv("SCHEMA_CACHE_TTL_SECONDS", "100")

    set_cached_schema_snapshot_id(tenant_id=1, snapshot_id="fp-1", now=100.0)
    set_cached_schema_snapshot_id(tenant_id=2, snapshot_id="fp-2", now=101.0)

    # Touch tenant 1 so tenant 2 becomes the eviction candidate.
    assert get_cached_schema_snapshot_id(tenant_id=1, now=101.5) == "fp-1"

    set_cached_schema_snapshot_id(tenant_id=3, snapshot_id="fp-3", now=102.0)

    assert get_cached_schema_snapshot_id(tenant_id=2, now=102.1) is None
    assert get_cached_schema_snapshot_id(tenant_id=1, now=102.1) == "fp-1"
    assert get_cached_schema_snapshot_id(tenant_id=3, now=102.1) == "fp-3"


def test_schema_cache_records_hit_and_miss_metrics():
    """Hit/miss metric counters should be emitted on cache lookup."""
    reset_schema_cache()
    with patch("agent.utils.schema_cache.agent_metrics.add_counter") as mock_add_counter:
        assert get_cached_schema_snapshot_id(tenant_id=5, now=10.0) is None
        set_cached_schema_snapshot_id(tenant_id=5, snapshot_id="fp-5", now=10.0)
        assert get_cached_schema_snapshot_id(tenant_id=5, now=10.1) == "fp-5"

    metric_names = [call.args[0] for call in mock_add_counter.call_args_list]
    assert "schema.cache.miss" in metric_names
    assert "schema.cache.hit" in metric_names


def test_schema_snapshot_churn_metric_emits_on_snapshot_id_change():
    """Churn metric should increment when tenant snapshot id changes."""
    reset_schema_cache()
    with patch("agent.utils.schema_cache.agent_metrics.add_counter") as mock_add_counter:
        set_cached_schema_snapshot_id(tenant_id=8, snapshot_id="fp-a", now=50.0)
        set_cached_schema_snapshot_id(tenant_id=8, snapshot_id="fp-a", now=51.0)
        set_cached_schema_snapshot_id(tenant_id=8, snapshot_id="fp-b", now=52.0)

    churn_calls = [
        call for call in mock_add_counter.call_args_list if call.args[0] == "schema.snapshot.churn"
    ]
    assert len(churn_calls) == 1


@pytest.mark.asyncio
async def test_schema_refresh_cooldown_raises_limit_exceeded(monkeypatch):
    """Refresh cooldown should throttle repeated refresh storms for a tenant."""
    reset_schema_cache()
    monkeypatch.setenv("SCHEMA_CACHE_TTL_SECONDS", "1")
    monkeypatch.setenv("SCHEMA_REFRESH_COOLDOWN_SECONDS", "10")

    current = {"now": 100.0}
    monkeypatch.setattr("agent.utils.schema_cache.time.monotonic", lambda: current["now"])
    calls = {"count": 0}

    async def refresh_fn():
        calls["count"] += 1
        return f"fp-{calls['count']}"

    first = await get_or_refresh_schema_snapshot_id(tenant_id=22, refresh_fn=refresh_fn)
    assert first == "fp-1"

    current["now"] = 101.0  # cache TTL expired, but cooldown still active
    with pytest.raises(SchemaRefreshLimitExceeded) as exc_info:
        await get_or_refresh_schema_snapshot_id(tenant_id=22, refresh_fn=refresh_fn)

    assert exc_info.value.retry_after_seconds > 0
    assert calls["count"] == 1
