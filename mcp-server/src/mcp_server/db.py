import os
from contextlib import asynccontextmanager
from typing import Optional

import asyncpg


class Database:
    """Manages asyncpg connection pool for PostgreSQL with tenant context."""

    _pool: Optional[asyncpg.Pool] = None

    @classmethod
    async def init(cls):
        """Initialize the connection pool with optimal settings for asyncpg."""
        db_host = os.getenv("DB_HOST", "localhost")
        db_port = int(os.getenv("DB_PORT", "5432"))
        db_name = os.getenv("DB_NAME", "pagila")
        db_user = os.getenv("DB_USER", "bi_agent_ro")
        db_pass = os.getenv("DB_PASS", "secure_agent_pass")

        dsn = f"postgresql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}"

        try:
            cls._pool = await asyncpg.create_pool(
                dsn,
                min_size=5,
                max_size=20,
                command_timeout=60,
                # Ensure we don't accidentally share state if a connection dies dirty
                server_settings={"application_name": "bi_agent_mcp"},
            )
            print(f"✓ Database connection pool established: {db_user}@{db_host}/{db_name}")
        except Exception as e:
            raise ConnectionError(f"Failed to create connection pool: {e}")

    @classmethod
    async def close(cls):
        """Close the connection pool."""
        if cls._pool:
            await cls._pool.close()
            print("✓ Database connection pool closed")

    @classmethod
    @asynccontextmanager
    async def get_connection(cls, tenant_id: Optional[int] = None):
        """Yield a connection with the tenant context securely set.

        Guarantees cleanup via transaction scoping.

        Args:
            tenant_id: Optional tenant identifier. If None, connection operates without RLS context.

        Yields:
            asyncpg.Connection: A connection with tenant context set for the transaction.
        """
        if cls._pool is None:
            raise RuntimeError("Database pool not initialized. Call Database.init() first.")

        async with cls._pool.acquire() as conn:
            # Start a transaction block.
            # Everything inside here is atomic.
            async with conn.transaction():
                if tenant_id is not None:
                    # set_config with is_local=True scopes the setting to this transaction.
                    # It will be automatically unset when the transaction block exits.
                    await conn.execute(
                        "SELECT set_config('app.current_tenant', $1, true)", str(tenant_id)
                    )

                # Yield the configured connection to the caller
                yield conn
                # Transaction commits/rolls back automatically here
                # Connection is returned to pool, tenant context is cleared
