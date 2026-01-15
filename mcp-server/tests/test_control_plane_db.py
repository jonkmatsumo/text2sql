"""Unit tests for ControlPlaneDatabase initialization."""

from unittest.mock import AsyncMock, patch

import asyncpg
import pytest
from mcp_server.config.control_plane import ControlPlaneDatabase


class TestControlPlaneDatabase:
    """Unit tests for ControlPlaneDatabase initialization."""

    @pytest.fixture(autouse=True)
    def reset_state(self):
        """Reset pool and isolation flag before and after each test."""
        ControlPlaneDatabase._pool = None
        ControlPlaneDatabase._isolation_enabled = False
        yield
        ControlPlaneDatabase._pool = None
        ControlPlaneDatabase._isolation_enabled = False

    @pytest.mark.asyncio
    async def test_init_applies_pinned_schema(self):
        """Ensure pinned_recommendations schema is applied on init."""
        mock_pool = AsyncMock(spec=asyncpg.Pool)
        mock_conn = AsyncMock()
        mock_acquire = AsyncMock()
        mock_acquire.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_acquire.__aexit__ = AsyncMock(return_value=False)
        mock_pool.acquire.return_value = mock_acquire

        env_vars = {
            "DB_ISOLATION_ENABLED": "true",
            "CONTROL_DB_HOST": "agent-control-db",
            "CONTROL_DB_PORT": "5432",
            "CONTROL_DB_NAME": "agent_control",
            "CONTROL_DB_USER": "postgres",
            "CONTROL_DB_PASSWORD": "control_password",
        }

        with patch(
            "mcp_server.config.control_plane.asyncpg.create_pool", new_callable=AsyncMock
        ) as mock_create:
            mock_create.return_value = mock_pool
            with patch(
                "common.config.env.os.getenv",
                side_effect=lambda key, default=None: env_vars.get(key, default),
            ):
                await ControlPlaneDatabase.init()

        executed_sql = " ".join(call.args[0] for call in mock_conn.execute.call_args_list)
        assert "CREATE TABLE IF NOT EXISTS pinned_recommendations" in executed_sql
        assert "CREATE OR REPLACE FUNCTION update_modified_column" in executed_sql
        assert "DROP TRIGGER IF EXISTS update_pinned_recos_modtime" in executed_sql
        assert "CREATE TRIGGER update_pinned_recos_modtime" in executed_sql

        # Verify order: Function before Trigger
        calls = [call.args[0] for call in mock_conn.execute.call_args_list]
        func_idx = next(
            i
            for i, c in enumerate(calls)
            if "CREATE OR REPLACE FUNCTION update_modified_column" in c
        )
        trigger_idx = next(
            i for i, c in enumerate(calls) if "CREATE TRIGGER update_pinned_recos_modtime" in c
        )
        assert func_idx < trigger_idx
