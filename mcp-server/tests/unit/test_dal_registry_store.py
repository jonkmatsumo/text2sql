from unittest.mock import AsyncMock, patch

import pytest

MOCK_EMBEDDING = [0.1, 0.2, 0.3]


class TestPostgresRegistryStore:
    """Test suite for Postgres Registry Store adapter."""

    @pytest.fixture
    def store(self):
        """Fixture for PostgresRegistryStore."""
        from dal.postgres.registry_store import PostgresRegistryStore

        return PostgresRegistryStore()

    @pytest.fixture
    def mock_db(self):
        """Fixture to mock Database."""
        with patch("dal.postgres.registry_store.Database") as mock:
            yield mock

    @pytest.mark.asyncio
    async def test_lookup_semantic_candidates_with_status(self, store, mock_db):
        """Test status filtering in semantic lookup."""
        mock_conn = AsyncMock()
        mock_db.get_connection.return_value.__aenter__.return_value = mock_conn
        mock_conn.fetch.return_value = []

        await store.lookup_semantic_candidates(
            MOCK_EMBEDDING, tenant_id=1, role="example", status="verified"
        )

        mock_conn.fetch.assert_called_once()
        args, kwargs = mock_conn.fetch.call_args
        query = args[0]
        query_args = args[1:]

        assert "status = $5" in query  # $1 vector, $2 threshold, $3 limit, $4 role, $5 status
        assert "verified" in query_args
