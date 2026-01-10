"""Control-Plane Database connection manager.

Manages a separate connection pool for the agent control-plane database,
which stores metadata, cache, policies, and evaluation data.

This pool is completely isolated from the query-target database to ensure
LLM-generated SQL never accesses control-plane data.
"""

import os
from contextlib import asynccontextmanager
from typing import Optional

import asyncpg


class ControlPlaneDatabase:
    """Manages connection pool for the control-plane database."""

    _pool: Optional[asyncpg.Pool] = None
    _isolation_enabled: bool = False

    @classmethod
    def is_enabled(cls) -> bool:
        """Check if database isolation is enabled."""
        return cls._isolation_enabled

    @classmethod
    async def init(cls):
        """Initialize control-plane connection pool."""
        # Check feature flag
        cls._isolation_enabled = os.getenv("DB_ISOLATION_ENABLED", "false").lower() == "true"

        if not cls._isolation_enabled:
            print("⚠ DB_ISOLATION_ENABLED=false, control-plane pool disabled")
            return

        # Control-plane Postgres Config
        db_host = os.getenv("CONTROL_DB_HOST", "agent-control-db")
        db_port = int(os.getenv("CONTROL_DB_PORT", "5432"))
        db_name = os.getenv("CONTROL_DB_NAME", "agent_control")
        db_user = os.getenv("CONTROL_DB_USER", "postgres")
        db_pass = os.getenv("CONTROL_DB_PASSWORD", "control_password")

        dsn = f"postgresql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}"

        try:
            cls._pool = await asyncpg.create_pool(
                dsn,
                min_size=2,
                max_size=10,
                command_timeout=30,
                server_settings={"application_name": "agent_control_plane"},
            )
            print(f"✓ Control-plane pool established: {db_user}@{db_host}/{db_name}")
        except Exception as e:
            print(f"✗ Failed to connect to control-plane DB: {e}")
            raise ConnectionError(f"Control-plane DB connection failed: {e}")

    @classmethod
    async def close(cls):
        """Close control-plane connection pool."""
        if cls._pool:
            await cls._pool.close()
            print("✓ Control-plane connection pool closed")
            cls._pool = None

    @classmethod
    @asynccontextmanager
    async def get_connection(cls, tenant_id: Optional[int] = None):
        """Yield a control-plane connection with optional tenant context.

        Args:
            tenant_id: Optional tenant identifier for RLS on control-plane tables.

        Yields:
            asyncpg.Connection: A connection to the control-plane database.
        """
        if not cls._isolation_enabled:
            # Fallback: import and use main Database pool
            from mcp_server.config.database import Database

            async with Database.get_connection(tenant_id) as conn:
                yield conn
            return

        if cls._pool is None:
            raise RuntimeError(
                "Control-plane pool not initialized. Call ControlPlaneDatabase.init() first."
            )

        async with cls._pool.acquire() as conn:
            async with conn.transaction():
                if tenant_id is not None:
                    await conn.execute(
                        "SELECT set_config('app.current_tenant', $1, true)", str(tenant_id)
                    )
                yield conn
