import os
from contextlib import asynccontextmanager
from typing import Optional

import asyncpg
from mcp_server.dal.interfaces import (
    CacheStore,
    ExampleStore,
    GraphStore,
    MetadataStore,
    SchemaIntrospector,
    SchemaStore,
)
from mcp_server.dal.memgraph import MemgraphStore


class Database:
    """Manages connection pools for PostgreSQL and Memgraph."""

    _pool: Optional[asyncpg.Pool] = None
    _graph_store: Optional[GraphStore] = None
    _cache_store: Optional[CacheStore] = None
    _example_store: Optional[ExampleStore] = None
    _schema_store: Optional[SchemaStore] = None
    _schema_introspector: Optional[SchemaIntrospector] = None
    _metadata_store: Optional[MetadataStore] = None

    @classmethod
    async def init(cls):
        """Initialize connection pools."""
        # Postgres Config
        db_host = os.getenv("DB_HOST", "localhost")
        db_port = int(os.getenv("DB_PORT", "5432"))
        db_name = os.getenv("DB_NAME", "pagila")
        db_user = os.getenv("DB_USER", "text2sql_ro")
        db_pass = os.getenv("DB_PASS", "secure_agent_pass")

        dsn = f"postgresql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}"

        # Memgraph Config
        graph_uri = os.getenv("MEMGRAPH_URI", "bolt://localhost:7687")
        graph_user = os.getenv("MEMGRAPH_USER", "")
        graph_pass = os.getenv("MEMGRAPH_PASSWORD", "")

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

            # 2. Init Memgraph
            cls._graph_store = MemgraphStore(graph_uri, graph_user, graph_pass)
            print(f"✓ Graph store connection established: {graph_uri}")

            # 3. Init Stores via Factory
            from mcp_server.dal.factory import (
                get_cache_store,
                get_example_store,
                get_metadata_store,
                get_schema_introspector,
                get_schema_store,
            )

            # Ensure operational schema exists
            await cls.ensure_schema()

            cls._cache_store = get_cache_store()
            cls._example_store = get_example_store()
            cls._schema_store = get_schema_store()
            cls._schema_introspector = get_schema_introspector()
            cls._metadata_store = get_metadata_store()

            print("✓ Stores initialized via DAL factory")

            # 4. Init Control-Plane Database (if enabled)
            from mcp_server.config.control_plane import ControlPlaneDatabase

            await ControlPlaneDatabase.init()

        except Exception as e:
            await cls.close()  # Cleanup partials
            raise ConnectionError(f"Failed to initialize databases: {e}")

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
                    CHECK (status IN ('RUNNING', 'COMPLETED', 'FAILED'))
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

        if cls._graph_store:
            cls._graph_store.close()
            try:
                print("✓ Graph store connection closed")
            except ValueError:
                pass
            cls._graph_store = None

        # Close control-plane pool
        from mcp_server.config.control_plane import ControlPlaneDatabase

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
    async def get_connection(cls, tenant_id: Optional[int] = None):
        """Yield a Postgres connection with the tenant context securely set.

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
