from contextlib import asynccontextmanager
from typing import Optional


class SnowflakeQueryTargetDatabase:
    """Snowflake query-target database placeholder (P2 skeleton)."""

    _account: Optional[str] = None
    _user: Optional[str] = None
    _password: Optional[str] = None
    _warehouse: Optional[str] = None
    _database: Optional[str] = None
    _schema: Optional[str] = None
    _role: Optional[str] = None
    _authenticator: Optional[str] = None

    @classmethod
    async def init(
        cls,
        account: Optional[str],
        user: Optional[str],
        password: Optional[str],
        warehouse: Optional[str],
        database: Optional[str],
        schema: Optional[str],
        role: Optional[str],
        authenticator: Optional[str],
    ) -> None:
        """Initialize Snowflake query-target config (no-op placeholder)."""
        cls._account = account
        cls._user = user
        cls._password = password
        cls._warehouse = warehouse
        cls._database = database
        cls._schema = schema
        cls._role = role
        cls._authenticator = authenticator

        missing = [
            name
            for name, value in {
                "SNOWFLAKE_ACCOUNT": account,
                "SNOWFLAKE_USER": user,
                "SNOWFLAKE_WAREHOUSE": warehouse,
                "SNOWFLAKE_DATABASE": database,
                "SNOWFLAKE_SCHEMA": schema,
            }.items()
            if not value
        ]
        if missing:
            missing_list = ", ".join(missing)
            raise ValueError(
                f"Snowflake query target missing required config: {missing_list}. "
                "Set SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_WAREHOUSE, "
                "SNOWFLAKE_DATABASE, and SNOWFLAKE_SCHEMA."
            )

    @classmethod
    async def close(cls) -> None:
        """Close Snowflake resources (no-op placeholder)."""
        return None

    @classmethod
    @asynccontextmanager
    async def get_connection(cls, *_args, **_kwargs):
        """Yield a Snowflake connection (not yet implemented)."""
        raise NotImplementedError("Snowflake query-target connection is not implemented yet.")
        yield
