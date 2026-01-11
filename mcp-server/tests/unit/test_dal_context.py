"""Unit tests for DAL context management."""

import pytest
from mcp_server.dal.util.context import get_current_tenant, set_current_tenant, tenant_context


class TestTenantContext:
    """Tests for tenant context isolation."""

    def test_default_context(self):
        """Test default state is None."""
        # Ensure clean state
        set_current_tenant(None)
        assert get_current_tenant() is None

    def test_set_get_context(self):
        """Test setting and getting context."""
        set_current_tenant(123)
        assert get_current_tenant() == 123

        set_current_tenant(None)
        assert get_current_tenant() is None

    def test_context_manager(self):
        """Test scoped context manager."""
        assert get_current_tenant() is None

        with tenant_context(456):
            assert get_current_tenant() == 456

            # Nested context
            with tenant_context(789):
                assert get_current_tenant() == 789

            # Back to outer context
            assert get_current_tenant() == 456

        # Back to original state (None)
        assert get_current_tenant() is None

    @pytest.mark.asyncio
    async def test_async_propagation(self):
        """Test context propagation across async boundaries."""
        import asyncio

        async def check_tenant(expected_id):
            await asyncio.sleep(0.01)
            assert get_current_tenant() == expected_id
            return True

        with tenant_context(999):
            # Should be visible inside async call
            result = await check_tenant(999)
            assert result is True

            # Concurrent tasks should maintain their context

            # Define a task wrapper to set context
            async def task_with_context(tid):
                with tenant_context(tid):
                    await asyncio.sleep(0.01)
                    assert get_current_tenant() == tid
                    return tid

            # Launch multiple tasks with different contexts
            results = await asyncio.gather(
                task_with_context(1), task_with_context(2), task_with_context(3)
            )
            assert results == [1, 2, 3]

            # Main task context should be preserved
            assert get_current_tenant() == 999
