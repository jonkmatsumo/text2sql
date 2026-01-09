from unittest.mock import AsyncMock, patch

import pytest
from mcp_server.dal.postgres import PgSemanticCache
from mcp_server.dal.types import CacheLookupResult

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
