"""Control-Plane Database connection manager.

Manages a separate connection pool for the agent control-plane database,
which stores metadata, cache, policies, and evaluation data.

This pool is completely isolated from the query-target database to ensure
LLM-generated SQL never accesses control-plane data.
"""

import logging
from contextlib import asynccontextmanager
from typing import Optional

import asyncpg

logger = logging.getLogger(__name__)


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
        from common.config.env import get_env_bool, get_env_int, get_env_str

        # Check feature flag for READ routing
        cls._isolation_enabled = get_env_bool("DB_ISOLATION_ENABLED", False)

        # Always attempt to connect to control-plane if configured (for dual-write)
        db_host = get_env_str("CONTROL_DB_HOST")

        if not db_host:
            if cls._isolation_enabled:
                print(
                    "⚠ DB_ISOLATION_ENABLED=true but CONTROL_DB_HOST not set. "
                    "Falling back to disabled."
                )
                cls._isolation_enabled = False
            return

        # Control-plane Postgres Config
        db_port = get_env_int("CONTROL_DB_PORT", 5432)
        db_name = get_env_str("CONTROL_DB_NAME", "agent_control")
        db_user = get_env_str("CONTROL_DB_USER", "postgres")
        db_pass = get_env_str("CONTROL_DB_PASSWORD", "control_password")

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

            # Schema validation (no DDL - migrations should be run separately)
            await cls._validate_schema()

        except Exception as e:
            msg = f"✗ Failed to connect to control-plane DB: {e}"
            print(msg)
            # If isolation is mandatory, raise error. If just dual-write, maybe soft fail?
            # For now, we raise if isolation is enabled, otherwise log warning.
            if cls._isolation_enabled:
                raise ConnectionError(f"Control-plane DB connection failed: {e}")

    @classmethod
    async def close(cls):
        """Close control-plane connection pool."""
        if cls._pool:
            await cls._pool.close()
            try:
                print("✓ Control-plane connection pool closed")
            except ValueError:
                pass
            cls._pool = None

    @classmethod
    async def _validate_schema(cls) -> None:
        """Validate that required tables exist.

        This method checks for the existence of required tables and logs warnings
        if they're missing. It does NOT create tables - migrations should be run
        separately via scripts/migrations/migrate.py.
        """
        if cls._pool is None:
            return

        required_tables = [
            "ops_jobs",
            "pinned_recommendations",
            "synth_generation_runs",
            "synth_templates",
        ]
        missing_tables = []

        async with cls._pool.acquire() as conn:
            for table in required_tables:
                exists = await conn.fetchval(
                    """
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables
                        WHERE table_schema = 'public' AND table_name = $1
                    )
                    """,
                    table,
                )
                if not exists:
                    missing_tables.append(table)

        if missing_tables:
            logger.warning(
                "Control-plane tables missing: %s. "
                "Run migrations: python scripts/migrations/migrate.py",
                ", ".join(missing_tables),
            )
        else:
            logger.info("Control-plane schema validation passed")

    @classmethod
    async def ensure_ops_jobs_schema(cls) -> None:
        """Ensure ops_jobs table exists in control-plane DB.

        DEPRECATED: This method runs DDL on startup which causes race conditions
        in multi-replica deployments. Use scripts/migrations/migrate.py instead.
        """
        import warnings

        warnings.warn(
            "ensure_ops_jobs_schema is deprecated. Use scripts/migrations/migrate.py",
            DeprecationWarning,
            stacklevel=2,
        )

        if cls._pool is None:
            return

        async with cls._pool.acquire() as conn:
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS ops_jobs (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    job_type TEXT NOT NULL,
                    status TEXT NOT NULL
                        CHECK (status IN ('PENDING', 'RUNNING', 'COMPLETED', 'FAILED')),
                    started_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                    finished_at TIMESTAMPTZ,
                    error_message TEXT,
                    payload JSONB DEFAULT '{}'::jsonb,
                    result JSONB DEFAULT '{}'::jsonb
                );
                """
            )
            logger.info("Ensured ops_jobs table exists")

    @classmethod
    async def ensure_pinned_recommendations_schema(cls) -> None:
        """Ensure pinned_recommendations schema exists in control-plane DB.

        DEPRECATED: This method runs DDL on startup which causes race conditions
        in multi-replica deployments. Use scripts/migrations/migrate.py instead.
        """
        import warnings

        warnings.warn(
            "ensure_pinned_recommendations_schema is deprecated. "
            "Use scripts/migrations/migrate.py",
            DeprecationWarning,
            stacklevel=2,
        )

        if cls._pool is None:
            return

        async with cls._pool.acquire() as conn:
            # 1. Create table
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS pinned_recommendations (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    tenant_id INT NOT NULL,
                    match_type VARCHAR(20) NOT NULL CHECK (match_type IN ('exact', 'contains')),
                    match_value TEXT NOT NULL,
                    registry_example_ids JSONB NOT NULL DEFAULT '[]'::jsonb,
                    priority INT NOT NULL DEFAULT 0,
                    enabled BOOLEAN NOT NULL DEFAULT TRUE,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                );
                """
            )

            # 2. Add indexes
            await conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_pinned_recos_tenant
                ON pinned_recommendations(tenant_id);
                """
            )

            await conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_pinned_recos_enabled
                ON pinned_recommendations(enabled);
                """
            )

            # 3. Define helper function for triggers
            await conn.execute(
                """
                CREATE OR REPLACE FUNCTION update_modified_column()
                RETURNS TRIGGER AS $$
                BEGIN
                    NEW.updated_at = NOW();
                    RETURN NEW;
                END;
                $$ language 'plpgsql';
                """
            )

            # 4. Create trigger (idempotent via DROP IF EXISTS)
            await conn.execute(
                """
                DROP TRIGGER IF EXISTS update_pinned_recos_modtime ON pinned_recommendations;
                CREATE TRIGGER update_pinned_recos_modtime
                    BEFORE UPDATE ON pinned_recommendations
                    FOR EACH ROW
                    EXECUTE PROCEDURE update_modified_column();
                """
            )

    @classmethod
    def is_configured(cls) -> bool:
        """Check if control-plane pool is active."""
        return cls._pool is not None

    @classmethod
    @asynccontextmanager
    async def get_connection(cls, tenant_id: Optional[int] = None):
        """Yield a connection based on isolation strategy (Read Path).

        If isolation enabled: returns Control Plane connection.
        If disabled: returns Main Database connection.
        """
        if not cls._isolation_enabled:
            # Fallback: import and use main Database pool
            from dal.database import Database

            async with Database.get_connection(tenant_id) as conn:
                yield conn
            return

        async with cls.get_direct_connection(tenant_id) as conn:
            yield conn

    @classmethod
    @asynccontextmanager
    async def get_direct_connection(cls, tenant_id: Optional[int] = None):
        """Yield a direct connection to control-plane (Write Path)."""
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
