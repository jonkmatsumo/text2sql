from contextlib import asynccontextmanager
from typing import Optional


class MysqlQueryTargetDatabase:
    """MySQL query-target database placeholder (P1b skeleton)."""

    _host: Optional[str] = None
    _port: int = 3306
    _db_name: Optional[str] = None
    _user: Optional[str] = None
    _password: Optional[str] = None

    @classmethod
    async def init(
        cls,
        host: Optional[str],
        port: int,
        db_name: Optional[str],
        user: Optional[str],
        password: Optional[str],
    ) -> None:
        """Initialize MySQL query-target config (no-op placeholder)."""
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
            }.items()
            if not value
        ]
        if missing:
            missing_list = ", ".join(missing)
            raise ValueError(
                f"MySQL query target missing required config: {missing_list}. "
                "Set DB_HOST, DB_NAME, and DB_USER."
            )

    @classmethod
    async def close(cls) -> None:
        """Close MySQL resources (no-op placeholder)."""
        return None

    @classmethod
    @asynccontextmanager
    async def get_connection(cls, *_args, **_kwargs):
        """Yield a MySQL connection (not yet implemented)."""
        raise NotImplementedError("MySQL query-target connection is not implemented yet.")
        yield
