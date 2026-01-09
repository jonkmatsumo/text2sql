import os
from contextlib import asynccontextmanager
from typing import Optional

import asyncpg
from mcp_server.dal.interfaces import CacheStore, ExampleStore, GraphStore
from mcp_server.dal.memgraph import MemgraphStore


class Database:
    """Manages connection pools for PostgreSQL and Memgraph."""

    _pool: Optional[asyncpg.Pool] = None
    _graph_store: Optional[GraphStore] = None
    _cache_store: Optional[CacheStore] = None
    _example_store: Optional[ExampleStore] = None

    @classmethod
    async def init(cls):
        """Initialize connection pools."""
        # Postgres Config
        db_host = os.getenv("DB_HOST", "localhost")
        db_port = int(os.getenv("DB_PORT", "5432"))
        db_name = os.getenv("DB_NAME", "pagila")
        db_user = os.getenv("DB_USER", "bi_agent_ro")
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

            # 3. Init CacheStore & ExampleStore (Postgres impl)
            # Avoid circular import at top level
            from mcp_server.dal.postgres import PgSemanticCache, PostgresExampleStore

            cls._cache_store = PgSemanticCache()
            print("✓ Cache store initialized")

            cls._example_store = PostgresExampleStore()
            print("✓ Example store initialized")

        except Exception as e:
            await cls.close()  # Cleanup partials
            raise ConnectionError(f"Failed to initialize databases: {e}")

    @classmethod
    async def close(cls):
        """Close connection pools."""
        if cls._pool:
            await cls._pool.close()
            print("✓ Database connection pool closed")
            cls._pool = None

        if cls._graph_store:
            cls._graph_store.close()
            print("✓ Graph store connection closed")
            cls._graph_store = None

        # Cache store (PgSemanticCache) doesn't hold its own connection,
        # it uses Database.get_connection, so no explicit close needed
        # but we clear reference
        cls._cache_store = None
        cls._example_store = None

    @classmethod
    def get_graph_store(cls) -> GraphStore:
        """Get the initialized graph store instance."""
        if cls._graph_store is None:
            raise RuntimeError("Graph store not initialized. Call Database.init() first.")
        return cls._graph_store

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
