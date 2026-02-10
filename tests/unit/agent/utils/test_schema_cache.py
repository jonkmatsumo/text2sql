"""Tests for schema snapshot cache."""

import pytest

from agent.utils.schema_cache import (
    MemorySchemaSnapshotCache,
    get_cached_schema_snapshot_id,
    get_schema_cache,
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
