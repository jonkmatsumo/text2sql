"""Unit tests for ControlPlaneDatabase initialization."""

import os
from unittest.mock import AsyncMock, patch

import asyncpg
import pytest

from dal.control_plane import ControlPlaneDatabase


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
    @pytest.mark.skipif(
        os.environ.get("CI") == "true",
        reason="Skipped in CI - schema validation test requires proper db mock setup",
    )
    async def test_init_validates_schema_without_ddl(self):
        """Ensure init() validates schema but does NOT run DDL.

        After the hardening changes, startup should only validate that tables
        exist, not create them. DDL should be run via migrations separately.
        """
        mock_pool = AsyncMock(spec=asyncpg.Pool)
        mock_conn = AsyncMock()
        mock_acquire = AsyncMock()
        mock_acquire.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_acquire.__aexit__ = AsyncMock(return_value=False)
        mock_pool.acquire.return_value = mock_acquire

        # Mock fetchval to return True (tables exist)
        mock_conn.fetchval = AsyncMock(return_value=True)

        env_vars = {
            "DB_ISOLATION_ENABLED": "true",
            "CONTROL_DB_HOST": "agent-control-db",
            "CONTROL_DB_PORT": "5432",
            "CONTROL_DB_NAME": "agent_control",
            "CONTROL_DB_USER": "postgres",
            "CONTROL_DB_PASSWORD": "control_password",
        }

        with patch("dal.control_plane.asyncpg.create_pool", new_callable=AsyncMock) as mock_create:
            mock_create.return_value = mock_pool
            with patch(
                "common.config.env.os.getenv",
                side_effect=lambda key, default=None: env_vars.get(key, default),
            ):
                await ControlPlaneDatabase.init()

        # Verify schema validation queries were made (SELECT, not CREATE)
        fetchval_calls = mock_conn.fetchval.call_args_list
        assert len(fetchval_calls) >= 2  # At least ops_jobs and pinned_recommendations

        # Verify NO DDL was executed
        execute_calls = mock_conn.execute.call_args_list
        for call in execute_calls:
            sql = call.args[0] if call.args else ""
            assert "CREATE TABLE" not in sql, "Startup should not run CREATE TABLE DDL"
            assert "CREATE TRIGGER" not in sql, "Startup should not run CREATE TRIGGER DDL"
