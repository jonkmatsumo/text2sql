"""Tests for schema snapshot cache."""

import asyncio

import pytest

from agent.utils.schema_cache import (
    MemorySchemaSnapshotCache,
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
