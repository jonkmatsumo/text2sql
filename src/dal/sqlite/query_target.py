from contextlib import asynccontextmanager
from typing import Optional


class SqliteQueryTargetDatabase:
    """SQLite query-target database placeholder (P1a skeleton)."""

    _db_path: Optional[str] = None

    @classmethod
    async def init(cls, db_path: Optional[str]) -> None:
        """Initialize SQLite query-target config (no-op placeholder)."""
        cls._db_path = db_path

    @classmethod
    async def close(cls) -> None:
        """Close SQLite resources (no-op placeholder)."""
        return None

    @classmethod
    @asynccontextmanager
    async def get_connection(cls, *_args, **_kwargs):
        """Yield a SQLite connection (not yet implemented)."""
        raise NotImplementedError("SQLite query-target connection is not implemented yet.")
        yield
