"""Concurrency tests for PolicyLoader."""

import asyncio
from unittest.mock import patch

import pytest

from agent.validation.policy_loader import PolicyLoader


class TestPolicyLoaderConcurrency:
    """Verify that PolicyLoader handles concurrent refreshes safely."""

    @pytest.fixture(autouse=True)
    def reset_loader(self):
        """Reset PolicyLoader state before each test."""
        PolicyLoader._policies = {}
        PolicyLoader._last_load_time = 0.0
        PolicyLoader._lock = None
        yield

    @pytest.mark.asyncio
    async def test_concurrent_get_policies_triggers_single_refresh(self):
        """Verify that multiple concurrent calls trigger only one refresh."""
        refresh_call_count = 0

        async def mock_refresh():
            nonlocal refresh_call_count
            refresh_call_count += 1
            # Simulate some I/O delay
            await asyncio.sleep(0.05)
            import time

            PolicyLoader._policies = {"test": "policy"}
            PolicyLoader._last_load_time = time.time()

        with patch.object(PolicyLoader, "_refresh_policies", side_effect=mock_refresh):
            # Launch multiple concurrent calls
            tasks = [PolicyLoader.get_policies() for _ in range(10)]
            results = await asyncio.gather(*tasks)

            # All calls should return the same policies
            for res in results:
                assert res == {"test": "policy"}

            # But _refresh_policies should have been called exactly once
            assert refresh_call_count == 1

    @pytest.mark.asyncio
    async def test_sequential_get_policies_after_ttl_triggers_new_refresh(self):
        """Verify that a new refresh is triggered after TTL expires."""
        refresh_call_count = 0

        async def mock_refresh():
            nonlocal refresh_call_count
            refresh_call_count += 1
            PolicyLoader._policies = {"test": f"policy_{refresh_call_count}"}
            import time

            PolicyLoader._last_load_time = time.time()

        with patch.object(PolicyLoader, "_refresh_policies", side_effect=mock_refresh):
            # First call
            await PolicyLoader.get_policies()
            assert refresh_call_count == 1

            # Immediate second call (cached)
            await PolicyLoader.get_policies()
            assert refresh_call_count == 1

            # Simulate TTL expiry
            PolicyLoader._last_load_time -= PolicyLoader._CACHE_TTL + 1

            # Third call (should trigger refresh)
            await PolicyLoader.get_policies()
            assert refresh_call_count == 2
