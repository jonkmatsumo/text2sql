from contextlib import asynccontextmanager
from typing import Optional

import asyncpg


class RedshiftQueryTargetDatabase:
    """Redshift query-target database using asyncpg."""

    _host: Optional[str] = None
    _port: int = 5439
    _db_name: Optional[str] = None
    _user: Optional[str] = None
    _password: Optional[str] = None
    _pool: Optional[asyncpg.Pool] = None

    @classmethod
    async def init(
        cls,
        host: Optional[str],
        port: int,
        db_name: Optional[str],
        user: Optional[str],
        password: Optional[str],
    ) -> None:
        """Initialize Redshift query-target config."""
        cls._host = host
        cls._port = port
        cls._db_name = db_name
        cls._user = user
        cls._password = password

        missing = [
            name
            for name, value in {
                "DB_HOST": host,
                "DB_NAME": db_name,
                "DB_USER": user,
                "DB_PASS": password,
            }.items()
            if not value
        ]
        if missing:
            missing_list = ", ".join(missing)
            raise ValueError(
                f"Redshift query target missing required config: {missing_list}. "
                "Set DB_HOST, DB_NAME, DB_USER, and DB_PASS."
            )
        if cls._pool is None:
            dsn = f"postgresql://{cls._user}:{cls._password}@{cls._host}:{cls._port}/{cls._db_name}"
            cls._pool = await asyncpg.create_pool(
                dsn,
                min_size=1,
                max_size=10,
                command_timeout=60,
                server_settings={"application_name": "bi_agent_redshift"},
            )

    @classmethod
    async def close(cls) -> None:
        """Close Redshift resources."""
        if cls._pool is not None:
            await cls._pool.close()
            cls._pool = None

    @classmethod
    @asynccontextmanager
    async def get_connection(cls, tenant_id: Optional[int] = None, read_only: bool = False):
        """Yield a Redshift connection wrapper (tenant context is a no-op)."""
        _ = tenant_id
        if cls._pool is None:
            raise RuntimeError(
                "Redshift pool not initialized. Call RedshiftQueryTargetDatabase.init()."
            )

        async with cls._pool.acquire() as conn:
            async with conn.transaction(readonly=read_only):
                yield conn
