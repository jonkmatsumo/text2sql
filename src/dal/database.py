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
    _query_target_capabilities = None
    _query_target_sync_max_rows: int = 0
    supports_tenant_enforcement: bool = True

    @classmethod
    async def init(cls):
        """Initialize connection pools."""
        from common.config.env import get_env_int, get_env_str
        from dal.capabilities import capabilities_for_provider
        from dal.query_target_config_source import (
            build_clickhouse_params,
            build_mysql_params,
            build_postgres_params,
            finalize_pending_config,
            guardrail_bool,
            guardrail_int,
            load_query_target_config_selection,
            resolve_secret_ref,
        )
        from dal.util.env import get_provider_env

        selection = await load_query_target_config_selection()
        pending_config = selection.pending
        active_config = selection.active
        runtime_config = pending_config or active_config

        if runtime_config is None:
            backend_override = get_env_str("QUERY_TARGET_BACKEND")
            if backend_override:
                cls._query_target_provider = get_provider_env(
                    "QUERY_TARGET_BACKEND",
                    default="postgres",
                    allowed={
                        "postgres",
                        "sqlite",
                        "mysql",
                        "snowflake",
                        "redshift",
                        "bigquery",
                        "athena",
                        "databricks",
                        "cockroachdb",
                        "duckdb",
                        "clickhouse",
                    },
                )
            else:
                cls._query_target_provider = get_provider_env(
                    "QUERY_TARGET_PROVIDER",
                    default="postgres",
                    allowed={
                        "postgres",
                        "sqlite",
                        "mysql",
                        "snowflake",
                        "redshift",
                        "bigquery",
                        "athena",
                        "databricks",
                        "cockroachdb",
                        "duckdb",
                        "clickhouse",
                    },
                )
        else:
            cls._query_target_provider = runtime_config.provider

        cls._query_target_capabilities = capabilities_for_provider(cls._query_target_provider)

        async def _init_query_target(runtime_config):
            if cls._query_target_provider == "sqlite":
                from dal.sqlite import SqliteQueryTargetDatabase

                if runtime_config:
                    max_rows = runtime_config.guardrails.get("max_rows")
                    await SqliteQueryTargetDatabase.init(
                        runtime_config.metadata["path"], max_rows=max_rows
                    )
                else:
                    sqlite_path = get_env_str("SQLITE_DB_PATH")
                    await SqliteQueryTargetDatabase.init(sqlite_path)
            elif cls._query_target_provider == "mysql":
                from dal.mysql import MysqlQueryTargetDatabase

                if runtime_config:
                    params = build_mysql_params(runtime_config.metadata, runtime_config.auth)
                    max_rows = runtime_config.guardrails.get("max_rows")
                    db_host = params["host"]
                    db_port = params["port"]
                    db_name = params["db_name"]
                    db_user = params["user"]
                    db_pass = params["password"]
                else:
                    db_host = get_env_str("DB_HOST")
                    db_port = get_env_int("DB_PORT", 3306)
                    db_name = get_env_str("DB_NAME")
                    db_user = get_env_str("DB_USER")
                    db_pass = get_env_str("DB_PASS")
                    max_rows = None
                await MysqlQueryTargetDatabase.init(
                    host=db_host,
                    port=db_port,
                    db_name=db_name,
                    user=db_user,
                    password=db_pass,
                    max_rows=max_rows,
                )
            elif cls._query_target_provider == "snowflake":
                from dal.snowflake import SnowflakeQueryTargetDatabase
                from dal.snowflake.config import SnowflakeConfig

                if runtime_config:
                    guardrails = runtime_config.guardrails
                    password = resolve_secret_ref(runtime_config.auth, "SNOWFLAKE_PASSWORD")
                    config = SnowflakeConfig(
                        account=runtime_config.metadata["account"],
                        user=runtime_config.metadata["user"],
                        password=password,
                        warehouse=runtime_config.metadata["warehouse"],
                        database=runtime_config.metadata["database"],
                        schema=runtime_config.metadata["schema"],
                        role=runtime_config.metadata.get("role"),
                        authenticator=runtime_config.metadata.get("authenticator"),
                        query_timeout_seconds=guardrail_int(
                            guardrails.get("query_timeout_seconds"), 30
                        ),
                        poll_interval_seconds=guardrail_int(
                            guardrails.get("poll_interval_seconds"), 1
                        ),
                        max_rows=guardrail_int(guardrails.get("max_rows"), 1000),
                        warn_after_seconds=guardrail_int(guardrails.get("warn_after_seconds"), 10),
                    )
                    await SnowflakeQueryTargetDatabase.init(config)
                else:
                    await SnowflakeQueryTargetDatabase.init(SnowflakeConfig.from_env())
            elif cls._query_target_provider == "redshift":
                from dal.redshift import RedshiftQueryTargetDatabase

                if runtime_config:
                    params = build_postgres_params(
                        runtime_config.metadata, runtime_config.auth, default_port=5439
                    )
                    max_rows = runtime_config.guardrails.get("max_rows")
                    db_host = params["host"]
                    db_port = params["port"]
                    db_name = params["db_name"]
                    db_user = params["user"]
                    db_pass = params["password"]
                else:
                    db_host = get_env_str("DB_HOST")
                    db_port = get_env_int("DB_PORT", 5439)
                    db_name = get_env_str("DB_NAME")
                    db_user = get_env_str("DB_USER")
                    db_pass = get_env_str("DB_PASS")
                    max_rows = None
                await RedshiftQueryTargetDatabase.init(
                    host=db_host,
                    port=db_port,
                    db_name=db_name,
                    user=db_user,
                    password=db_pass,
                    max_rows=max_rows,
                )
            elif cls._query_target_provider == "bigquery":
                from dal.bigquery import BigQueryConfig, BigQueryQueryTargetDatabase

                if runtime_config:
                    guardrails = runtime_config.guardrails
                    config = BigQueryConfig(
                        project=runtime_config.metadata["project"],
                        dataset=runtime_config.metadata["dataset"],
                        location=runtime_config.metadata.get("location"),
                        query_timeout_seconds=guardrail_int(
                            guardrails.get("query_timeout_seconds"), 30
                        ),
                        poll_interval_seconds=guardrail_int(
                            guardrails.get("poll_interval_seconds"), 1
                        ),
                        max_rows=guardrail_int(guardrails.get("max_rows"), 1000),
                    )
                    await BigQueryQueryTargetDatabase.init(config)
                else:
                    await BigQueryQueryTargetDatabase.init(BigQueryConfig.from_env())
            elif cls._query_target_provider == "athena":
                from dal.athena import AthenaConfig, AthenaQueryTargetDatabase

                if runtime_config:
                    guardrails = runtime_config.guardrails
                    config = AthenaConfig(
                        region=runtime_config.metadata["region"],
                        workgroup=runtime_config.metadata["workgroup"],
                        output_location=runtime_config.metadata["output_location"],
                        database=runtime_config.metadata["database"],
                        query_timeout_seconds=guardrail_int(
                            guardrails.get("query_timeout_seconds"), 30
                        ),
                        poll_interval_seconds=guardrail_int(
                            guardrails.get("poll_interval_seconds"), 1
                        ),
                        max_rows=guardrail_int(guardrails.get("max_rows"), 1000),
                    )
                    await AthenaQueryTargetDatabase.init(config)
                else:
                    await AthenaQueryTargetDatabase.init(AthenaConfig.from_env())
            elif cls._query_target_provider == "databricks":
                from dal.databricks import DatabricksConfig, DatabricksQueryTargetDatabase

                if runtime_config:
                    guardrails = runtime_config.guardrails
                    token = resolve_secret_ref(runtime_config.auth, "DATABRICKS_TOKEN")
                    if not token:
                        raise ValueError("Missing Databricks token reference.")
                    config = DatabricksConfig(
                        host=runtime_config.metadata["host"],
                        token=token,
                        warehouse_id=runtime_config.metadata["warehouse_id"],
                        catalog=runtime_config.metadata["catalog"],
                        schema=runtime_config.metadata["schema"],
                        query_timeout_seconds=guardrail_int(
                            guardrails.get("query_timeout_seconds"), 30
                        ),
                        poll_interval_seconds=guardrail_int(
                            guardrails.get("poll_interval_seconds"), 1
                        ),
                        max_rows=guardrail_int(guardrails.get("max_rows"), 1000),
                    )
                    await DatabricksQueryTargetDatabase.init(config)
                else:
                    await DatabricksQueryTargetDatabase.init(DatabricksConfig.from_env())
            elif cls._query_target_provider == "duckdb":
                from dal.duckdb import DuckDBConfig, DuckDBQueryTargetDatabase

                if runtime_config:
                    guardrails = runtime_config.guardrails
                    config = DuckDBConfig(
                        path=runtime_config.metadata["path"],
                        query_timeout_seconds=guardrail_int(
                            guardrails.get("query_timeout_seconds"), 30
                        ),
                        max_rows=guardrail_int(guardrails.get("max_rows"), 1000),
                        read_only=guardrail_bool(guardrails.get("read_only"), True),
                    )
                    await DuckDBQueryTargetDatabase.init(config)
                else:
                    await DuckDBQueryTargetDatabase.init(DuckDBConfig.from_env())
            elif cls._query_target_provider == "clickhouse":
                from dal.clickhouse import ClickHouseConfig, ClickHouseQueryTargetDatabase

                if runtime_config:
                    guardrails = runtime_config.guardrails
                    params = build_clickhouse_params(runtime_config.metadata, runtime_config.auth)
                    config = ClickHouseConfig(
                        host=params["host"],
                        port=params["port"],
                        database=params["database"],
                        user=params["user"],
                        password=params["password"],
                        secure=params["secure"],
                        query_timeout_seconds=guardrail_int(
                            guardrails.get("query_timeout_seconds"), 30
                        ),
                        max_rows=guardrail_int(guardrails.get("max_rows"), 1000),
                    )
                    await ClickHouseQueryTargetDatabase.init(config)
                else:
                    await ClickHouseQueryTargetDatabase.init(ClickHouseConfig.from_env())
            else:
                # Postgres Config
                if runtime_config:
                    default_port = 5432 if cls._query_target_provider == "postgres" else 26257
                    params = build_postgres_params(
                        runtime_config.metadata,
                        runtime_config.auth,
                        default_port=default_port,
                    )
                    db_host = params["host"]
                    db_port = params["port"]
                    db_name = params["db_name"]
                    db_user = params["user"]
                    db_pass = params["password"]
                    cls._query_target_sync_max_rows = guardrail_int(
                        runtime_config.guardrails.get("max_rows"), 0
                    )
                else:
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

        init_config = runtime_config
        try:
            await _init_query_target(init_config)
        except Exception:
            if pending_config and init_config is pending_config:
                await finalize_pending_config(
                    pending_config,
                    active_config,
                    success=False,
                    error_message="Query-target initialization failed.",
                )
                if active_config:
                    init_config = active_config
                    cls._query_target_provider = init_config.provider
                    cls._query_target_capabilities = capabilities_for_provider(
                        cls._query_target_provider
                    )
                    await _init_query_target(init_config)
                else:
                    raise
            else:
                raise

        if pending_config and init_config is pending_config:
            await finalize_pending_config(
                pending_config,
                active_config,
                success=True,
            )

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
        if cls._query_target_provider == "bigquery":
            from dal.bigquery import BigQueryQueryTargetDatabase

            await BigQueryQueryTargetDatabase.close()
        if cls._query_target_provider == "athena":
            from dal.athena import AthenaQueryTargetDatabase

            await AthenaQueryTargetDatabase.close()
        if cls._query_target_provider == "databricks":
            from dal.databricks import DatabricksQueryTargetDatabase

            await DatabricksQueryTargetDatabase.close()
        if cls._query_target_provider == "duckdb":
            from dal.duckdb import DuckDBQueryTargetDatabase

            await DuckDBQueryTargetDatabase.close()
        if cls._query_target_provider == "clickhouse":
            from dal.clickhouse import ClickHouseQueryTargetDatabase

            await ClickHouseQueryTargetDatabase.close()

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
    def get_query_target_provider(cls) -> str:
        """Return the active query-target provider."""
        return cls._query_target_provider

    @classmethod
    def get_provider_identity(cls) -> str:
        """Return canonical provider identity from capabilities when available."""
        caps = cls._query_target_capabilities
        if caps is not None:
            provider_name_raw = getattr(caps, "provider_name", None)
            if not isinstance(provider_name_raw, str):
                provider_name_raw = ""
            provider_name = provider_name_raw.strip().lower()
            if provider_name and provider_name not in {"unknown", "unspecified"}:
                return provider_name
        return cls._query_target_provider

    @classmethod
    def get_query_target_capabilities(cls):
        """Get capability flags for the active query-target backend."""
        if cls._query_target_capabilities is None:
            raise RuntimeError(
                "Query-target capabilities not initialized. Call Database.init() first."
            )
        return cls._query_target_capabilities

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

            async with SqliteQueryTargetDatabase.get_connection(
                tenant_id=tenant_id, read_only=read_only
            ) as conn:
                yield conn
            return
        if cls._query_target_provider == "mysql":
            from dal.mysql import MysqlQueryTargetDatabase

            async with MysqlQueryTargetDatabase.get_connection(
                tenant_id=tenant_id, read_only=read_only
            ) as conn:
                yield conn
            return
        if cls._query_target_provider == "snowflake":
            from dal.snowflake import SnowflakeQueryTargetDatabase

            async with SnowflakeQueryTargetDatabase.get_connection(
                tenant_id=tenant_id, read_only=read_only
            ) as conn:
                yield conn
            return
        if cls._query_target_provider == "redshift":
            from dal.redshift import RedshiftQueryTargetDatabase

            async with RedshiftQueryTargetDatabase.get_connection(
                tenant_id=tenant_id, read_only=read_only
            ) as conn:
                yield conn
            return
        if cls._query_target_provider == "bigquery":
            from dal.bigquery import BigQueryQueryTargetDatabase

            async with BigQueryQueryTargetDatabase.get_connection(
                tenant_id=tenant_id, read_only=read_only
            ) as conn:
                yield conn
            return
        if cls._query_target_provider == "athena":
            from dal.athena import AthenaQueryTargetDatabase

            async with AthenaQueryTargetDatabase.get_connection(
                tenant_id=tenant_id, read_only=read_only
            ) as conn:
                yield conn
            return
        if cls._query_target_provider == "databricks":
            from dal.databricks import DatabricksQueryTargetDatabase

            async with DatabricksQueryTargetDatabase.get_connection(
                tenant_id=tenant_id, read_only=read_only
            ) as conn:
                yield conn
            return
        if cls._query_target_provider == "duckdb":
            from dal.duckdb import DuckDBQueryTargetDatabase

            async with DuckDBQueryTargetDatabase.get_connection(
                tenant_id=tenant_id, read_only=read_only
            ) as conn:
                yield conn
            return
        if cls._query_target_provider == "clickhouse":
            from dal.clickhouse import ClickHouseQueryTargetDatabase

            async with ClickHouseQueryTargetDatabase.get_connection(
                tenant_id=tenant_id, read_only=read_only
            ) as conn:
                yield conn
            return

        if cls._pool is None:
            raise RuntimeError("Database pool not initialized. Call Database.init() first.")

        async with cls._pool.acquire() as conn:
            if cls.get_query_target_capabilities().supports_transactions:
                # Start a transaction block.
                # Everything inside here is atomic.
                async with conn.transaction(readonly=read_only):
                    if tenant_id is not None:
                        # set_config with is_local=True scopes the setting to this transaction.
                        # It will be automatically unset when the transaction block exits.
                        await conn.execute(
                            "SELECT set_config('app.current_tenant', $1, true)",
                            str(tenant_id),
                        )

                    # Yield the configured connection to the caller
                    from dal.tracing import TracedAsyncpgConnection, trace_enabled
                    from dal.util.row_limits import get_sync_max_rows

                    sync_max_rows = cls._query_target_sync_max_rows or get_sync_max_rows()
                    if trace_enabled() or sync_max_rows or read_only:
                        yield TracedAsyncpgConnection(
                            conn,
                            provider=cls._query_target_provider,
                            execution_model=cls.get_query_target_capabilities().execution_model,
                            max_rows=sync_max_rows,
                            read_only=read_only,
                        )
                    else:
                        yield conn
                    # Transaction commits/rolls back automatically here
                    # Connection is returned to pool, tenant context is cleared
            else:
                if tenant_id is not None:
                    await conn.execute(
                        "SELECT set_config('app.current_tenant', $1, true)", str(tenant_id)
                    )
                from dal.tracing import TracedAsyncpgConnection, trace_enabled
                from dal.util.row_limits import get_sync_max_rows

                sync_max_rows = cls._query_target_sync_max_rows or get_sync_max_rows()
                if trace_enabled() or sync_max_rows or read_only:
                    yield TracedAsyncpgConnection(
                        conn,
                        provider=cls._query_target_provider,
                        execution_model=cls.get_query_target_capabilities().execution_model,
                        max_rows=sync_max_rows,
                        read_only=read_only,
                    )
                else:
                    yield conn

    @classmethod
    async def fetch_query(
        cls,
        sql: str,
        tenant_id: Optional[int] = None,
        params: Optional[list] = None,
        include_columns: bool = False,
    ):
        """Fetch rows with optional column metadata when supported."""
        from dal.query_result import QueryResult

        async with cls.get_connection(tenant_id=tenant_id, read_only=True) as conn:
            fetch_with_columns = getattr(conn, "fetch_with_columns", None)
            prepare = getattr(conn, "prepare", None)
            supports_fetch_with_columns = (
                include_columns
                and callable(fetch_with_columns)
                and "fetch_with_columns" in type(conn).__dict__
            )
            supports_prepare = (
                include_columns and callable(prepare) and "prepare" in type(conn).__dict__
            )

            result = None
            if params:
                if supports_fetch_with_columns:
                    rows, columns = await fetch_with_columns(sql, *params)
                    result = QueryResult(rows=rows, columns=columns)
                elif supports_prepare:
                    from dal.util.column_metadata import columns_from_asyncpg_attributes

                    statement = await prepare(sql)
                    rows = await statement.fetch(*params)
                    columns = columns_from_asyncpg_attributes(statement.get_attributes())
                    result = QueryResult(rows=[dict(row) for row in rows], columns=columns)
                else:
                    rows = await conn.fetch(sql, *params)
                    result = QueryResult(rows=rows, columns=None)
            else:
                if supports_fetch_with_columns:
                    rows, columns = await fetch_with_columns(sql)
                    result = QueryResult(rows=rows, columns=columns)
                elif supports_prepare:
                    from dal.util.column_metadata import columns_from_asyncpg_attributes

                    statement = await prepare(sql)
                    rows = await statement.fetch()
                    columns = columns_from_asyncpg_attributes(statement.get_attributes())
                    result = QueryResult(rows=[dict(row) for row in rows], columns=columns)
                else:
                    rows = await conn.fetch(sql)
                    result = QueryResult(rows=rows, columns=None)

            if result and hasattr(conn, "last_truncated"):
                result.is_truncated = conn.last_truncated
                result.partial_reason = getattr(conn, "last_truncated_reason", None)

            return result
