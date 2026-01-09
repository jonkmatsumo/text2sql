from unittest.mock import AsyncMock, patch

import pytest
from mcp_server.dal.postgres import PgSemanticCache
from mcp_server.models.dal_types import CacheLookupResult

MOCK_EMBEDDING = [0.1, 0.2, 0.3]


class TestPgSemanticCache:
    """Test suite for Postgres Semantic Cache adapter."""

    @pytest.fixture
    def cache(self):
        """Fixture for PgSemanticCache."""
        return PgSemanticCache()

    @pytest.fixture
    def mock_db(self):
        """Fixture to mock Database."""
        with patch("mcp_server.dal.postgres.Database") as mock:
            yield mock

    @pytest.mark.asyncio
    async def test_lookup_hit(self, cache, mock_db):
        """Test lookup with a cache hit."""
        mock_conn = AsyncMock()
        mock_db.get_connection.return_value.__aenter__.return_value = mock_conn

        # Mock row return
        mock_row = {"cache_id": 123, "generated_sql": "SELECT * FROM t", "similarity": 0.98}
        mock_conn.fetchrow.return_value = mock_row

        result = await cache.lookup(MOCK_EMBEDDING, tenant_id=1)

        assert isinstance(result, CacheLookupResult)
        assert result.cache_id == "123"
        assert result.value == "SELECT * FROM t"
        assert result.similarity == 0.98

        # Verify SQL param formatting (format_vector_for_postgres mock or check str)
        mock_conn.fetchrow.assert_called_once()

    @pytest.mark.asyncio
    async def test_lookup_miss(self, cache, mock_db):
        """Test lookup with no results."""
        mock_conn = AsyncMock()
        mock_db.get_connection.return_value.__aenter__.return_value = mock_conn
        mock_conn.fetchrow.return_value = None

        result = await cache.lookup(MOCK_EMBEDDING, tenant_id=1)
        assert result is None

    @pytest.mark.asyncio
    async def test_record_hit(self, cache, mock_db):
        """Test record_hit executes update."""
        mock_conn = AsyncMock()
        mock_db.get_connection.return_value.__aenter__.return_value = mock_conn

        await cache.record_hit("123", tenant_id=1)

        mock_conn.execute.assert_called_once()
        args = mock_conn.execute.call_args[0]
        assert "UPDATE semantic_cache" in args[0]
        assert args[1] == 123  # Int conversion

    @pytest.mark.asyncio
    async def test_store(self, cache, mock_db):
        """Test store executes insert."""
        mock_conn = AsyncMock()
        mock_db.get_connection.return_value.__aenter__.return_value = mock_conn

        await cache.store("query", "sql", MOCK_EMBEDDING, tenant_id=1)

        mock_conn.execute.assert_called_once()
        args = mock_conn.execute.call_args[0]
        assert "INSERT INTO semantic_cache" in args[0]


class TestPostgresExampleStore:
    """Test suite for Postgres Example Store adapter."""

    @pytest.fixture
    def store(self):
        """Fixture for PostgresExampleStore."""
        from mcp_server.dal.postgres import PostgresExampleStore

        return PostgresExampleStore()

    @pytest.fixture
    def mock_db(self):
        """Fixture to mock Database."""
        with patch("mcp_server.dal.postgres.Database") as mock:
            yield mock

    @pytest.mark.asyncio
    async def test_fetch_all_examples(self, store, mock_db):
        """Test fetching examples maps to Example objects."""
        mock_conn = AsyncMock()
        mock_db.get_connection.return_value.__aenter__.return_value = mock_conn

        mock_rows = [
            {
                "id": 1,
                "question": "Q1",
                "sql_query": "SELECT 1",
                "embedding": "[0.1, 0.2]",
            },
            {
                "id": 2,
                "question": "Q2",
                "sql_query": "SELECT 2",
                "embedding": [0.3, 0.4],  # Test list input
            },
        ]
        mock_conn.fetch.return_value = mock_rows

        examples = await store.fetch_all_examples()

        assert len(examples) == 2
        assert examples[0].id == 1
        assert examples[0].question == "Q1"
        assert examples[0].embedding == [0.1, 0.2]

        assert examples[1].id == 2
        assert examples[1].embedding == [0.3, 0.4]

        mock_conn.fetch.assert_called_once()


class TestPostgresSchemaStore:
    """Test suite for Postgres Schema Store adapter."""

    @pytest.fixture
    def store(self):
        """Fixture for PostgresSchemaStore."""
        from mcp_server.dal.postgres import PostgresSchemaStore

        return PostgresSchemaStore()

    @pytest.fixture
    def mock_db(self):
        """Fixture to mock Database."""
        with patch("mcp_server.dal.postgres.Database") as mock:
            yield mock

    @pytest.mark.asyncio
    async def test_fetch_schema_embeddings(self, store, mock_db):
        """Test fetching schemas maps to SchemaEmbedding objects."""
        mock_conn = AsyncMock()
        mock_db.get_connection.return_value.__aenter__.return_value = mock_conn

        mock_rows = [
            {
                "table_name": "t1",
                "schema_text": "schema1",
                "embedding": "[0.1, 0.2]",
            },
            {
                "table_name": "t2",
                "schema_text": "schema2",
                "embedding": [0.3, 0.4],
            },
        ]
        mock_conn.fetch.return_value = mock_rows

        schemas = await store.fetch_schema_embeddings()

        assert len(schemas) == 2
        assert schemas[0].table_name == "t1"
        assert schemas[0].schema_text == "schema1"
        assert schemas[0].embedding == [0.1, 0.2]

        assert schemas[1].table_name == "t2"
        assert schemas[1].embedding == [0.3, 0.4]

        mock_conn.fetch.assert_called_once()


class TestPostgresSchemaIntrospector:
    """Test suite for Postgres Schema Introspector adapter."""

    @pytest.fixture
    def introspector(self):
        """Fixture for PostgresSchemaIntrospector."""
        from mcp_server.dal.postgres import PostgresSchemaIntrospector

        return PostgresSchemaIntrospector()

    @pytest.fixture
    def mock_db(self):
        """Fixture to mock Database."""
        with patch("mcp_server.dal.postgres.Database") as mock:
            yield mock

    @pytest.mark.asyncio
    async def test_list_table_names(self, introspector, mock_db):
        """Test listing tables."""
        mock_conn = AsyncMock()
        mock_db.get_connection.return_value.__aenter__.return_value = mock_conn

        mock_rows = [{"table_name": "t1"}, {"table_name": "t2"}]
        mock_conn.fetch.return_value = mock_rows

        tables = await introspector.list_table_names()

        assert tables == ["t1", "t2"]
        mock_conn.fetch.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_table_def(self, introspector, mock_db):
        """Test getting full table definition."""
        mock_conn = AsyncMock()
        mock_db.get_connection.return_value.__aenter__.return_value = mock_conn

        # Mock columns
        mock_cols = [
            {"column_name": "id", "data_type": "int", "is_nullable": "NO", "ordinal_position": 1},
            {
                "column_name": "name",
                "data_type": "text",
                "is_nullable": "YES",
                "ordinal_position": 2,
            },
        ]

        # Mock FKs
        mock_fks = [
            {"column_name": "role_id", "foreign_table_name": "roles", "foreign_column_name": "id"}
        ]

        # Use side_effect to return different results for consecutive fetch calls
        # 1. columns query
        # 2. fks query
        mock_conn.fetch.side_effect = [mock_cols, mock_fks]

        table_def = await introspector.get_table_def("users")

        assert table_def.name == "users"

        # Check columns
        assert len(table_def.columns) == 2
        assert table_def.columns[0].name == "id"
        assert not table_def.columns[0].is_nullable
        assert table_def.columns[1].name == "name"
        assert table_def.columns[1].is_nullable

        # Check FKs
        assert len(table_def.foreign_keys) == 1
        assert table_def.foreign_keys[0].column_name == "role_id"
        assert table_def.foreign_keys[0].foreign_table_name == "roles"
