import os
from typing import Optional

import asyncpg


class Database:
    """Manages asyncpg connection pool for PostgreSQL."""

    _pool: Optional[asyncpg.Pool] = None

    @classmethod
    async def init(cls):
        """Initialize the connection pool."""
        db_host = os.getenv("DB_HOST", "localhost")
        db_port = int(os.getenv("DB_PORT", "5432"))
        db_name = os.getenv("DB_NAME", "pagila")
        db_user = os.getenv("DB_USER", "bi_agent_ro")
        db_pass = os.getenv("DB_PASS", "secure_agent_pass")

        dsn = f"postgresql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}"

        try:
            cls._pool = await asyncpg.create_pool(dsn, min_size=2, max_size=10, command_timeout=60)
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
    async def get_connection(cls):
        """Acquire a connection from the pool."""
        if cls._pool is None:
            raise RuntimeError("Database pool not initialized. Call Database.init() first.")
        return await cls._pool.acquire()

    @classmethod
    async def release_connection(cls, conn):
        """Release a connection back to the pool."""
        if cls._pool:
            await cls._pool.release(conn)
