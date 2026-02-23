import logging
from contextlib import asynccontextmanager
from typing import Optional

import asyncpg
from opentelemetry import trace

from common.interfaces import (
    CacheStore,
    ExampleStore,
    GraphStore,
    MetadataStore,
    SchemaIntrospector,
    SchemaStore,
)
from common.observability.metrics import mcp_metrics
from dal.postgres_sandbox import (
    SANDBOX_FAILURE_NONE,
    PostgresExecutionSandbox,
    PostgresSandboxExecutionError,
    build_postgres_sandbox_metadata,
)
from dal.session_guardrails import (
    RESTRICTED_SESSION_MODE_OFF,
    RESTRICTED_SESSION_MODE_SET_LOCAL_CONFIG,
    SESSION_GUARDRAIL_APPLIED,
    SESSION_GUARDRAIL_MISCONFIGURED,
    SESSION_GUARDRAIL_SKIPPED,
    PostgresSessionGuardrailSettings,
    SessionGuardrailPolicyError,
    build_session_guardrail_metadata,
    sanitize_execution_role_name,
)

logger = logging.getLogger(__name__)


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
    _postgres_extension_capability_cache: dict[str, tuple[bool, bool]] = {}
    _postgres_extension_warning_emitted: set[str] = set()
    _postgres_session_guardrail_settings: Optional[PostgresSessionGuardrailSettings] = None

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
        cls._postgres_extension_capability_cache = {}
        cls._postgres_extension_warning_emitted = set()
        cls._load_postgres_session_guardrail_settings()

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
        cls._postgres_extension_capability_cache = {}
        cls._postgres_extension_warning_emitted = set()
        cls._postgres_session_guardrail_settings = None

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

    @staticmethod
    def _quote_postgres_identifier(identifier: str) -> str:
        """Return a safely quoted Postgres identifier."""
        return '"' + identifier.replace('"', '""') + '"'

    @classmethod
    def _load_postgres_session_guardrail_settings(cls) -> PostgresSessionGuardrailSettings:
        """Resolve and validate guardrail settings at initialization time."""
        settings = PostgresSessionGuardrailSettings.from_env()
        settings.validate_basic(cls._query_target_provider)
        cls._postgres_session_guardrail_settings = settings
        return settings

    @classmethod
    def _get_postgres_session_guardrail_settings(cls) -> PostgresSessionGuardrailSettings:
        """Return startup-resolved settings with a lazy fallback for tests."""
        settings = cls._postgres_session_guardrail_settings
        if settings is not None:
            return settings
        try:
            return cls._load_postgres_session_guardrail_settings()
        except ValueError as exc:
            raise SessionGuardrailPolicyError(
                reason_code="session_guardrail_misconfigured",
                outcome=SESSION_GUARDRAIL_MISCONFIGURED,
                message=str(exc),
                envelope_metadata=build_session_guardrail_metadata(
                    applied=False,
                    outcome=SESSION_GUARDRAIL_MISCONFIGURED,
                    execution_role_applied=False,
                    execution_role_name=None,
                    restricted_session_mode=RESTRICTED_SESSION_MODE_OFF,
                    capability_mismatch="session_guardrail_misconfigured",
                ),
            ) from exc

    @classmethod
    async def _probe_postgres_dangerous_extension_capabilities(
        cls,
        conn: asyncpg.Connection,
        *,
        cache_key: str,
    ) -> tuple[bool, bool]:
        """Return cached (installed, executable) capability for dblink extension."""
        cached = cls._postgres_extension_capability_cache.get(cache_key)
        if cached is not None:
            return cached

        installed = False
        accessible = False
        try:
            row = await conn.fetchrow(
                """
                SELECT
                    EXISTS (
                        SELECT 1
                        FROM pg_extension
                        WHERE extname = 'dblink'
                    ) AS dblink_installed,
                    EXISTS (
                        SELECT 1
                        FROM pg_extension ext
                        JOIN pg_depend dep
                            ON dep.refobjid = ext.oid
                            AND dep.deptype = 'e'
                        JOIN pg_proc proc
                            ON proc.oid = dep.objid
                        WHERE ext.extname = 'dblink'
                            AND has_function_privilege(proc.oid, 'EXECUTE')
                    ) AS dblink_accessible
                """
            )
            if row is not None:
                installed = bool(row["dblink_installed"])
                accessible = bool(row["dblink_accessible"])
        except Exception:
            logger.debug(
                "Failed Postgres dangerous-extension capability probe for cache key '%s'.",
                cache_key,
                exc_info=True,
            )

        cls._postgres_extension_capability_cache[cache_key] = (installed, accessible)
        return installed, accessible

    @classmethod
    def _record_postgres_extension_capability_signals(
        cls,
        *,
        cache_key: str,
        execution_role: str,
        dblink_installed: bool,
        dblink_accessible: bool,
    ) -> None:
        """Emit low-cardinality telemetry and warning metrics for dblink accessibility."""
        sanitized_execution_role = sanitize_execution_role_name(execution_role) or "unknown_role"
        span = trace.get_current_span()
        if span is not None and span.is_recording():
            span.set_attribute("db.postgres.execution_role", sanitized_execution_role)
            span.set_attribute("db.postgres.extension.dblink.installed", dblink_installed)
            span.set_attribute("db.postgres.extension.dblink.accessible", dblink_accessible)

        if not dblink_installed or not dblink_accessible:
            return
        if cache_key in cls._postgres_extension_warning_emitted:
            return

        cls._postgres_extension_warning_emitted.add(cache_key)
        logger.warning(
            "Postgres execution role '%s' can execute dblink extension functions. "
            "AST-level SQL guardrails may be bypassed by extension-mediated calls.",
            sanitized_execution_role,
        )
        mcp_metrics.add_counter(
            "mcp.postgres.dangerous_extension_accessible_total",
            description=(
                "Count of sessions where dblink extension functions are executable under the "
                "configured Postgres execution role."
            ),
            attributes={
                "provider": "postgres",
                "execution_role": sanitized_execution_role,
                "extension": "dblink",
            },
        )

    @classmethod
    async def _apply_postgres_restricted_session(
        cls,
        conn: asyncpg.Connection,
        *,
        read_only: bool,
    ) -> dict[str, object]:
        """Apply optional Postgres-only transaction-local session hardening."""
        if not read_only:
            return build_session_guardrail_metadata(
                applied=False,
                outcome=SESSION_GUARDRAIL_SKIPPED,
                execution_role_applied=False,
                execution_role_name=None,
                restricted_session_mode=RESTRICTED_SESSION_MODE_OFF,
            )

        from common.config.env import get_env_bool, get_env_int, get_env_str

        settings = cls._get_postgres_session_guardrail_settings()
        restricted_session_enabled = settings.restricted_session_enabled
        execution_role_enabled = settings.execution_role_enabled

        if not restricted_session_enabled and not execution_role_enabled:
            return build_session_guardrail_metadata(
                applied=False,
                outcome=SESSION_GUARDRAIL_SKIPPED,
                execution_role_applied=False,
                execution_role_name=settings.execution_role_name,
                restricted_session_mode=RESTRICTED_SESSION_MODE_OFF,
            )

        capabilities = cls.get_query_target_capabilities()
        settings.validate_capabilities(
            provider=cls._query_target_provider,
            supports_restricted_session=bool(
                getattr(capabilities, "supports_restricted_session", False)
            ),
            supports_execution_role=bool(getattr(capabilities, "supports_execution_role", False)),
        )

        span = trace.get_current_span()
        if span is not None and span.is_recording():
            span.set_attribute(
                "db.postgres.guardrails.capability.supports_restricted_session",
                bool(getattr(capabilities, "supports_restricted_session", False)),
            )
            span.set_attribute(
                "db.postgres.guardrails.capability.supports_execution_role",
                bool(getattr(capabilities, "supports_execution_role", False)),
            )

        if cls._query_target_provider != "postgres":
            return build_session_guardrail_metadata(
                applied=False,
                outcome=SESSION_GUARDRAIL_SKIPPED,
                execution_role_applied=False,
                execution_role_name=settings.execution_role_name,
                restricted_session_mode=RESTRICTED_SESSION_MODE_OFF,
            )

        metadata = build_session_guardrail_metadata(
            applied=False,
            outcome=SESSION_GUARDRAIL_SKIPPED,
            execution_role_applied=False,
            execution_role_name=settings.execution_role_name,
            restricted_session_mode=RESTRICTED_SESSION_MODE_OFF,
        )

        def _timeout_value(env_name: str, default_ms: int) -> Optional[str]:
            timeout_ms = get_env_int(env_name, default_ms)
            if timeout_ms is None or timeout_ms <= 0:
                return None
            return f"{timeout_ms}ms"

        if restricted_session_enabled:
            await conn.execute("SELECT set_config('default_transaction_read_only', 'on', true)")
            metadata = build_session_guardrail_metadata(
                applied=True,
                outcome=SESSION_GUARDRAIL_APPLIED,
                execution_role_applied=bool(metadata["execution_role_applied"]),
                execution_role_name=settings.execution_role_name,
                restricted_session_mode=RESTRICTED_SESSION_MODE_SET_LOCAL_CONFIG,
            )

            statement_timeout = _timeout_value("POSTGRES_RESTRICTED_STATEMENT_TIMEOUT_MS", 15000)
            if statement_timeout:
                await conn.execute(
                    "SELECT set_config('statement_timeout', $1, true)",
                    statement_timeout,
                )

            lock_timeout = _timeout_value("POSTGRES_RESTRICTED_LOCK_TIMEOUT_MS", 5000)
            if lock_timeout:
                await conn.execute(
                    "SELECT set_config('lock_timeout', $1, true)",
                    lock_timeout,
                )

            idle_in_txn_timeout = _timeout_value(
                "POSTGRES_RESTRICTED_IDLE_IN_TRANSACTION_SESSION_TIMEOUT_MS", 15000
            )
            if idle_in_txn_timeout:
                await conn.execute(
                    "SELECT set_config('idle_in_transaction_session_timeout', $1, true)",
                    idle_in_txn_timeout,
                )

            search_path = (get_env_str("POSTGRES_RESTRICTED_SEARCH_PATH", "") or "").strip()
            if search_path:
                await conn.execute(
                    "SELECT set_config('search_path', $1, true)",
                    search_path,
                )

        if execution_role_enabled:
            execution_role = settings.execution_role_name
            if execution_role:
                quoted_role = cls._quote_postgres_identifier(execution_role)
                try:
                    await conn.execute(f"SET LOCAL ROLE {quoted_role}")
                except Exception as exc:
                    raise PostgresSandboxExecutionError(
                        "Failed to apply Postgres execution role in transaction sandbox.",
                        failure_reason="ROLE_SWITCH_FAILURE",
                    ) from exc
                metadata = build_session_guardrail_metadata(
                    applied=True,
                    outcome=SESSION_GUARDRAIL_APPLIED,
                    execution_role_applied=True,
                    execution_role_name=execution_role,
                    restricted_session_mode=str(metadata["restricted_session_mode"]),
                )

                cache_key = execution_role.lower()
                dblink_installed, dblink_accessible = (
                    await cls._probe_postgres_dangerous_extension_capabilities(
                        conn,
                        cache_key=cache_key,
                    )
                )
                cls._record_postgres_extension_capability_signals(
                    cache_key=cache_key,
                    execution_role=execution_role,
                    dblink_installed=dblink_installed,
                    dblink_accessible=dblink_accessible,
                )

                if (
                    dblink_installed
                    and dblink_accessible
                    and bool(get_env_bool("POSTGRES_DANGEROUS_EXTENSION_STRICT_MODE", False))
                ):
                    raise PermissionError(
                        "Execution role has dblink EXECUTE permissions while strict mode "
                        "is enabled."
                    )

        return metadata

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
            sandbox_metadata = build_postgres_sandbox_metadata(
                applied=False,
                rollback=False,
                failure_reason=SANDBOX_FAILURE_NONE,
            )
            if cls.get_query_target_capabilities().supports_transactions:
                use_postgres_sandbox = cls._query_target_provider == "postgres"
                if use_postgres_sandbox:
                    sandbox_metadata = build_postgres_sandbox_metadata(
                        applied=True,
                        rollback=False,
                        failure_reason=SANDBOX_FAILURE_NONE,
                    )
                transaction_scope = (
                    PostgresExecutionSandbox(
                        conn,
                        read_only=read_only,
                        metadata_sink=sandbox_metadata,
                    )
                    if use_postgres_sandbox
                    else conn.transaction(readonly=read_only)
                )
                async with transaction_scope:
                    session_guardrail_metadata = await cls._apply_postgres_restricted_session(
                        conn, read_only=read_only
                    )

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
                            session_guardrail_metadata=session_guardrail_metadata,
                            postgres_sandbox_metadata=sandbox_metadata,
                        )
                    else:
                        try:
                            setattr(conn, "session_guardrail_metadata", session_guardrail_metadata)
                            setattr(conn, "postgres_sandbox_metadata", sandbox_metadata)
                        except Exception:
                            pass
                        yield conn
                    # Transaction commits/rolls back automatically here
                    # Connection is returned to pool, tenant context is cleared
            else:
                session_guardrail_metadata = await cls._apply_postgres_restricted_session(
                    conn, read_only=read_only
                )
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
                        session_guardrail_metadata=session_guardrail_metadata,
                        postgres_sandbox_metadata=sandbox_metadata,
                    )
                else:
                    try:
                        setattr(conn, "session_guardrail_metadata", session_guardrail_metadata)
                        setattr(conn, "postgres_sandbox_metadata", sandbox_metadata)
                    except Exception:
                        pass
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
