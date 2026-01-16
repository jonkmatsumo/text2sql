"""Unit tests for ContextAwareExecutor."""

import asyncio

import pytest
from mcp_server.utils.context_aware_executor import (
    ContextAwareExecutor,
    run_in_executor_with_context,
)

from dal.context import get_current_tenant, set_current_tenant


class TestContextAwareExecutor:
    """Test safe context propagation."""

    @pytest.mark.asyncio
    async def test_submit_propagates_context(self):
        """Verify submit captures and restores context."""
        executor = ContextAwareExecutor(max_workers=1)

        # Set context in main thread
        set_current_tenant(123)

        def get_tenant_in_thread():
            return get_current_tenant()

        # Submit to thread
        future = executor.submit(get_tenant_in_thread)
        result = future.result()

        assert result == 123
        executor.shutdown()

    @pytest.mark.asyncio
    async def test_run_in_executor_helper(self):
        """Verify helper function propagates context."""
        # Set context
        set_current_tenant(456)

        def blocking_task(arg):
            return (get_current_tenant(), arg)

        # Run with default executor (None)
        # Note: run_in_executor_with_context manually wraps in context,
        # so it should work even with standard ThreadPoolExecutor.
        result_tenant, result_arg = await run_in_executor_with_context(None, blocking_task, "test")

        assert result_tenant == 456
        assert result_arg == "test"

    @pytest.mark.asyncio
    async def test_context_isolation(self):
        """Verify contexts don't leak between tasks."""
        executor = ContextAwareExecutor(max_workers=2)

        async def task(tenant_id):
            set_current_tenant(tenant_id)

            def check_context():
                import time

                time.sleep(0.01)  # Simulate work
                return get_current_tenant()

            loop = asyncio.get_running_loop()
            return await loop.run_in_executor(executor, check_context)

        # Run two concurrent tasks with different tenants
        results = await asyncio.gather(task(1), task(2))

        assert results == [1, 2]
        executor.shutdown()
