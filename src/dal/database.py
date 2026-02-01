from contextlib import asynccontextmanager
from typing import Optional

import asyncpg

from common.interfaces import (
    CacheStore,
    ExampleStore,
    GraphStore,
    MetadataStore,
    SchemaIntrospector,
    SchemaStore,
)


class Database:
    """Manages connection pools for PostgreSQL and Memgraph."""

    _pool: Optional[asyncpg.Pool] = None
    _graph_store: Optional[GraphStore] = None
    _cache_store: Optional[CacheStore] = None
    _example_store: Optional[ExampleStore] = None
    _schema_store: Optional[SchemaStore] = None
    _schema_introspector: Optional[SchemaIntrospector] = None
    _metadata_store: Optional[MetadataStore] = None
    _query_target_provider: str = "postgres"

    @classmethod
    async def init(cls):
        """Initialize connection pools."""
        from common.config.env import get_env_int, get_env_str
        from dal.util.env import get_provider_env

        backend_override = get_env_str("QUERY_TARGET_BACKEND")
        if backend_override:
            cls._query_target_provider = get_provider_env(
                "QUERY_TARGET_BACKEND",
                default="postgres",
                allowed={"postgres", "sqlite", "mysql", "snowflake", "redshift"},
            )
        else:
            cls._query_target_provider = get_provider_env(
                "QUERY_TARGET_PROVIDER",
                default="postgres",
                allowed={"postgres", "sqlite", "mysql", "snowflake", "redshift"},
            )

        if cls._query_target_provider == "sqlite":
            from dal.sqlite import SqliteQueryTargetDatabase

            sqlite_path = get_env_str("SQLITE_DB_PATH")
            await SqliteQueryTargetDatabase.init(sqlite_path)
        elif cls._query_target_provider == "mysql":
            from dal.mysql import MysqlQueryTargetDatabase

            db_host = get_env_str("DB_HOST")
            db_port = get_env_int("DB_PORT", 3306)
            db_name = get_env_str("DB_NAME")
            db_user = get_env_str("DB_USER")
            db_pass = get_env_str("DB_PASS")
            await MysqlQueryTargetDatabase.init(
                host=db_host,
                port=db_port,
                db_name=db_name,
                user=db_user,
                password=db_pass,
            )
        elif cls._query_target_provider == "snowflake":
            from dal.snowflake import SnowflakeQueryTargetDatabase
            from dal.snowflake.config import SnowflakeConfig

            await SnowflakeQueryTargetDatabase.init(SnowflakeConfig.from_env())
        elif cls._query_target_provider == "redshift":
            from dal.redshift import RedshiftQueryTargetDatabase

            db_host = get_env_str("DB_HOST")
            db_port = get_env_int("DB_PORT", 5439)
            db_name = get_env_str("DB_NAME")
            db_user = get_env_str("DB_USER")
            db_pass = get_env_str("DB_PASS")
            await RedshiftQueryTargetDatabase.init(
                host=db_host,
                port=db_port,
                db_name=db_name,
                user=db_user,
                password=db_pass,
            )
        else:
            # Postgres Config
            db_host = get_env_str("DB_HOST", "localhost")
            db_port = get_env_int("DB_PORT", 5432)
            from common.config.dataset import get_default_db_name

            db_name = get_env_str("DB_NAME", get_default_db_name())
            db_user = get_env_str("DB_USER", "text2sql_ro")
            db_pass = get_env_str("DB_PASS", "secure_agent_pass")

            dsn = f"postgresql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}"

            try:
                # 1. Init Postgres
                cls._pool = await asyncpg.create_pool(
                    dsn,
                    min_size=5,
                    max_size=20,
                    command_timeout=60,
                    server_settings={"application_name": "bi_agent_mcp"},
                )
                print(f"✓ Database connection pool established: {db_user}@{db_host}/{db_name}")
            except Exception as e:
                await cls.close()  # Cleanup partials
                raise ConnectionError(f"Failed to initialize databases: {e}")

        # 2. Init Stores via Factory
        from dal.factory import (
            get_cache_store,
            get_example_store,
            get_graph_store,
            get_metadata_store,
            get_schema_introspector,
            get_schema_store,
        )

        cls._graph_store = get_graph_store()
        print("✓ Graph store connection established (via factory)")

        # Ensure operational schema exists (Postgres-only)
        if cls._query_target_provider == "postgres":
            await cls.ensure_schema()

        cls._cache_store = get_cache_store()
        cls._example_store = get_example_store()
        cls._schema_store = get_schema_store()
        cls._schema_introspector = get_schema_introspector()
        cls._metadata_store = get_metadata_store()

        print("✓ Stores initialized via DAL factory")

        # 4. Init Control-Plane Database (if enabled)
        from dal.control_plane import ControlPlaneDatabase

        await ControlPlaneDatabase.init()

    @classmethod
    async def ensure_schema(cls):
        """Ensure operational schema tables exist."""
        if cls._pool is None:
            return

        async with cls._pool.acquire() as conn:
            # Table for NLP patterns (synonyms -> canonical values)
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS nlp_patterns (
                    id TEXT NOT NULL,          -- Canonical ID (e.g., "G")
                    label TEXT NOT NULL,       -- Entity Label (e.g., "RATING")
                    pattern TEXT NOT NULL,     -- The pattern/synonym (e.g., "general audiences")
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    deleted_at TIMESTAMP,
                    PRIMARY KEY (label, pattern)
                );
                """
            )
            print("✓ Operational schema ensured (nlp_patterns)")

            # Table for Pattern Generation Runs
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS nlp_pattern_runs (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    started_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                    completed_at TIMESTAMPTZ,
                    status TEXT NOT NULL,
                    target_table TEXT,
                    config_snapshot JSONB DEFAULT '{}'::jsonb,
                    error_message TEXT,
                    metrics JSONB DEFAULT '{}'::jsonb,
                    CHECK (status IN (
                        'RUNNING', 'COMPLETED', 'FAILED', 'AWAITING_REVIEW', 'ROLLED_BACK'
                    ))
                );
                """
            )

            # Table for Run-Pattern Association
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS nlp_pattern_run_items (
                    run_id UUID NOT NULL REFERENCES nlp_pattern_runs(id),
                    pattern_id TEXT NOT NULL,
                    -- pattern_id is the "Canonical Value".
                    -- We link via (label, pattern) which is the PK of nlp_patterns.
                    pattern_label TEXT NOT NULL,
                    pattern_text TEXT NOT NULL,
                    action TEXT NOT NULL,
                    CHECK (
                        action IN ('CREATED', 'UPDATED', 'UNCHANGED', 'DELETED')
                    ),
                    FOREIGN KEY (
                        pattern_label, pattern_text
                    ) REFERENCES nlp_patterns(label, pattern),
                    PRIMARY KEY (run_id, pattern_label, pattern_text)
                );
                """
            )
            print("✓ Operational schema ensured (nlp_pattern_runs, nlp_pattern_run_items)")

    @classmethod
    async def close(cls):
        """Close connection pools."""
        if cls._pool:
            await cls._pool.close()
            try:
                print("✓ Database connection pool closed")
            except ValueError:
                pass
            cls._pool = None

        if cls._query_target_provider == "sqlite":
            from dal.sqlite import SqliteQueryTargetDatabase

            await SqliteQueryTargetDatabase.close()
        if cls._query_target_provider == "mysql":
            from dal.mysql import MysqlQueryTargetDatabase

            await MysqlQueryTargetDatabase.close()
        if cls._query_target_provider == "snowflake":
            from dal.snowflake import SnowflakeQueryTargetDatabase

            await SnowflakeQueryTargetDatabase.close()
        if cls._query_target_provider == "redshift":
            from dal.redshift import RedshiftQueryTargetDatabase

            await RedshiftQueryTargetDatabase.close()

        if cls._graph_store:
            cls._graph_store.close()
            try:
                print("✓ Graph store connection closed")
            except ValueError:
                pass
            cls._graph_store = None

        # Close control-plane pool
        from dal.control_plane import ControlPlaneDatabase

        await ControlPlaneDatabase.close()

        # Cache store (PgSemanticCache) doesn't hold its own connection,
        # it uses Database.get_connection, so no explicit close needed
        # but we clear reference
        cls._cache_store = None
        cls._example_store = None
        cls._schema_store = None
        cls._schema_introspector = None
        cls._metadata_store = None

    @classmethod
    def get_graph_store(cls) -> GraphStore:
        """Get the initialized graph store instance."""
        if cls._graph_store is None:
            raise RuntimeError("Graph store not initialized. Call Database.init() first.")
        return cls._graph_store

    @classmethod
    def get_cache_store(cls) -> CacheStore:
        """Get the initialized cache store instance."""
        if cls._cache_store is None:
            raise RuntimeError("Cache store not initialized. Call Database.init() first.")
        return cls._cache_store

    @classmethod
    def get_example_store(cls) -> ExampleStore:
        """Get the initialized example store instance."""
        if cls._example_store is None:
            raise RuntimeError("Example store not initialized. Call Database.init() first.")
        return cls._example_store

    @classmethod
    def get_schema_store(cls) -> SchemaStore:
        """Get the initialized schema store instance."""
        if cls._schema_store is None:
            raise RuntimeError("Schema store not initialized. Call Database.init() first.")
        return cls._schema_store

    @classmethod
    def get_schema_introspector(cls) -> SchemaIntrospector:
        """Get the initialized schema introspector instance."""
        if cls._schema_introspector is None:
            raise RuntimeError("Schema introspector not initialized. Call Database.init() first.")
        return cls._schema_introspector

    @classmethod
    def get_metadata_store(cls) -> MetadataStore:
        """Get the initialized metadata store instance."""
        if cls._metadata_store is None:
            raise RuntimeError("Metadata store not initialized. Call Database.init() first.")
        return cls._metadata_store

    @classmethod
    @asynccontextmanager
    async def get_connection(cls, tenant_id: Optional[int] = None, read_only: bool = False):
        """Yield a query-target connection with optional tenant context.

        Guarantees cleanup via transaction scoping.

        Args:
            tenant_id: Optional tenant identifier. If None, connection operates without RLS context.

        Yields:
            asyncpg.Connection: A connection with tenant context set for the transaction.
        """
        if cls._query_target_provider == "sqlite":
            from dal.sqlite import SqliteQueryTargetDatabase

            async with SqliteQueryTargetDatabase.get_connection(tenant_id=tenant_id) as conn:
                yield conn
            return
        if cls._query_target_provider == "mysql":
            from dal.mysql import MysqlQueryTargetDatabase

            async with MysqlQueryTargetDatabase.get_connection(tenant_id=tenant_id) as conn:
                yield conn
            return
        if cls._query_target_provider == "snowflake":
            from dal.snowflake import SnowflakeQueryTargetDatabase

            async with SnowflakeQueryTargetDatabase.get_connection(tenant_id=tenant_id) as conn:
                yield conn
            return
        if cls._query_target_provider == "redshift":
            from dal.redshift import RedshiftQueryTargetDatabase

            async with RedshiftQueryTargetDatabase.get_connection(tenant_id=tenant_id) as conn:
                yield conn
            return

        if cls._pool is None:
            raise RuntimeError("Database pool not initialized. Call Database.init() first.")

        async with cls._pool.acquire() as conn:
            # Start a transaction block.
            # Everything inside here is atomic.
            async with conn.transaction(readonly=read_only):
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
