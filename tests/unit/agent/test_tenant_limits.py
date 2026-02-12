"""Tests for tenant-scoped concurrency limiting."""

import asyncio

import pytest

from common.tenancy.limits import TenantConcurrencyLimiter, TenantConcurrencyLimitExceeded


@pytest.mark.asyncio
async def test_same_tenant_limit_exceeded_returns_retry_hint():
    """Second concurrent run for same tenant should fail fast with retry hint."""
    limiter = TenantConcurrencyLimiter(
        per_tenant_limit=1,
        max_tracked_tenants=10,
        idle_ttl_seconds=60.0,
        retry_after_seconds=1.5,
    )
    release = asyncio.Event()
    started = asyncio.Event()

    async def _holder() -> None:
        async with limiter.acquire(tenant_id=17):
            started.set()
            await release.wait()

    task = asyncio.create_task(_holder())
    await started.wait()
    try:
        with pytest.raises(TenantConcurrencyLimitExceeded) as exc_info:
            async with limiter.acquire(tenant_id=17):
                pass
        assert exc_info.value.limit == 1
        assert exc_info.value.active_runs == 1
        assert exc_info.value.retry_after_seconds == 1.5
    finally:
        release.set()
        await task


@pytest.mark.asyncio
async def test_different_tenants_do_not_block_each_other():
    """Concurrent runs for different tenants should proceed independently."""
    limiter = TenantConcurrencyLimiter(
        per_tenant_limit=1,
        max_tracked_tenants=10,
        idle_ttl_seconds=60.0,
        retry_after_seconds=1.0,
    )
    release = asyncio.Event()
    started = asyncio.Event()

    async def _holder() -> None:
        async with limiter.acquire(tenant_id=1):
            started.set()
            await release.wait()

    task = asyncio.create_task(_holder())
    await started.wait()
    try:
        async with limiter.acquire(tenant_id=2) as lease:
            assert lease.tenant_id == 2
            assert lease.limit == 1
    finally:
        release.set()
        await task


@pytest.mark.asyncio
async def test_cancellation_releases_tenant_slot():
    """Cancelled in-flight run should release the tenant semaphore slot."""
    limiter = TenantConcurrencyLimiter(
        per_tenant_limit=1,
        max_tracked_tenants=10,
        idle_ttl_seconds=60.0,
        retry_after_seconds=1.0,
    )
    started = asyncio.Event()

    async def _long_running() -> None:
        async with limiter.acquire(tenant_id=9):
            started.set()
            await asyncio.sleep(30)

    task = asyncio.create_task(_long_running())
    await started.wait()

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    # Slot should be available again after cancellation cleanup.
    async with limiter.acquire(tenant_id=9):
        pass
