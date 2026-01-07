"""Tests for semantic caching."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from mcp_server.cache import (
    SIMILARITY_THRESHOLD,
    get_cache_stats,
    lookup_cache,
    update_cache,
    update_cache_access,
)


class TestLookupCache:
    """Unit tests for lookup_cache function."""

    @pytest.mark.asyncio
    async def test_lookup_cache_miss(self):
        """Test that cache returns None when cache is empty."""
        mock_conn = AsyncMock()
        mock_conn.fetchrow = AsyncMock(return_value=None)

        # Setup async context manager mock
        mock_get_cm = AsyncMock()
        mock_get_cm.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_get_cm.__aexit__ = AsyncMock(return_value=False)
        mock_get = MagicMock(return_value=mock_get_cm)

        mock_embedding = [0.1] * 384
        mock_pg_vector = "[0.1,0.1,...]"

        with patch("mcp_server.cache.Database.get_connection", mock_get):
            with patch("mcp_server.cache.RagEngine.embed_text", return_value=mock_embedding):
                with patch(
                    "mcp_server.cache.format_vector_for_postgres", return_value=mock_pg_vector
                ):
                    cached = await lookup_cache("What is the total revenue?", tenant_id=1)

                    assert cached is None
                    # Verify called with tenant_id
                    mock_get.assert_called_once()
                    # Check both positional and keyword args
                    call_args = mock_get.call_args
                    assert call_args[0] == (1,) or call_args[1].get("tenant_id") == 1

    @pytest.mark.asyncio
    async def test_lookup_cache_hit(self):
        """Test that cache returns cached SQL when similarity >= threshold."""
        mock_conn = AsyncMock()
        mock_row = {
            "cache_id": 1,
            "generated_sql": "SELECT SUM(amount) FROM payment;",
            "similarity": 0.96,
        }
        mock_conn.fetchrow = AsyncMock(return_value=mock_row)

        # Setup async context manager mock
        mock_get_cm = AsyncMock()
        mock_get_cm.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_get_cm.__aexit__ = AsyncMock(return_value=False)
        mock_get = MagicMock(return_value=mock_get_cm)

        mock_embedding = [0.1] * 384
        mock_pg_vector = "[0.1,0.1,...]"

        with patch("mcp_server.cache.Database.get_connection", mock_get):
            with patch("mcp_server.cache.RagEngine.embed_text", return_value=mock_embedding):
                with patch(
                    "mcp_server.cache.format_vector_for_postgres", return_value=mock_pg_vector
                ):
                    with patch("mcp_server.cache.update_cache_access", new_callable=AsyncMock):
                        cached = await lookup_cache("What is the total revenue?", tenant_id=1)

                        assert cached == "SELECT SUM(amount) FROM payment;"
                        # Verify called with tenant_id
                        mock_get.assert_called_once()
                        # Check both positional and keyword args
                        call_args = mock_get.call_args
                        assert call_args[0] == (1,) or call_args[1].get("tenant_id") == 1

    @pytest.mark.asyncio
    async def test_lookup_cache_uses_context_manager(self):
        """Verify database connection uses context manager pattern."""
        mock_conn = AsyncMock()
        mock_conn.fetchrow = AsyncMock(return_value=None)

        # Setup async context manager mock
        mock_get_cm = AsyncMock()
        mock_get_cm.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_get_cm.__aexit__ = AsyncMock(return_value=False)
        mock_get = MagicMock(return_value=mock_get_cm)

        mock_embedding = [0.1] * 384
        mock_pg_vector = "[0.1,0.1,...]"

        with patch("mcp_server.cache.Database.get_connection", mock_get):
            with patch("mcp_server.cache.RagEngine.embed_text", return_value=mock_embedding):
                with patch(
                    "mcp_server.cache.format_vector_for_postgres", return_value=mock_pg_vector
                ):
                    await lookup_cache("test query", tenant_id=1)

                    # Verify context manager was used
                    mock_get.assert_called_once()
                    # Check both positional and keyword args
                    call_args = mock_get.call_args
                    assert call_args[0] == (1,) or call_args[1].get("tenant_id") == 1
                    mock_get_cm.__aenter__.assert_called_once()
                    mock_get_cm.__aexit__.assert_called_once()


class TestUpdateCache:
    """Unit tests for update_cache function."""

    @pytest.mark.asyncio
    async def test_update_cache(self):
        """Verify cache insertion."""
        mock_conn = AsyncMock()
        mock_conn.execute = AsyncMock()

        # Setup async context manager mock
        mock_get_cm = AsyncMock()
        mock_get_cm.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_get_cm.__aexit__ = AsyncMock(return_value=False)
        mock_get = MagicMock(return_value=mock_get_cm)

        mock_embedding = [0.1] * 384
        mock_pg_vector = "[0.1,0.1,...]"

        with patch("mcp_server.cache.Database.get_connection", mock_get):
            with patch("mcp_server.cache.RagEngine.embed_text", return_value=mock_embedding):
                with patch(
                    "mcp_server.cache.format_vector_for_postgres", return_value=mock_pg_vector
                ):
                    await update_cache(
                        "What is the total revenue?",
                        "SELECT SUM(amount) FROM payment;",
                        tenant_id=1,
                    )

                    mock_get.assert_called_once()
                    # Check both positional and keyword args
                    call_args = mock_get.call_args
                    assert call_args[0] == (1,) or call_args[1].get("tenant_id") == 1
                    mock_conn.execute.assert_called_once()


class TestUpdateCacheAccess:
    """Unit tests for update_cache_access function."""

    @pytest.mark.asyncio
    async def test_update_cache_access(self):
        """Verify hit count and timestamp updates."""
        mock_conn = AsyncMock()
        mock_conn.execute = AsyncMock()

        # Setup async context manager mock
        mock_get_cm = AsyncMock()
        mock_get_cm.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_get_cm.__aexit__ = AsyncMock(return_value=False)
        mock_get = MagicMock(return_value=mock_get_cm)

        with patch("mcp_server.cache.Database.get_connection", mock_get):
            await update_cache_access(cache_id=1, tenant_id=1)

            mock_get.assert_called_once()
            # Check both positional and keyword args
            call_args = mock_get.call_args
            assert call_args[0] == (1,) or call_args[1].get("tenant_id") == 1
            mock_conn.execute.assert_called_once()
            # Verify UPDATE query was called
            call_args = mock_conn.execute.call_args[0][0]
            assert "UPDATE semantic_cache" in call_args
            assert "hit_count = hit_count + 1" in call_args


class TestGetCacheStats:
    """Unit tests for get_cache_stats function."""

    @pytest.mark.asyncio
    async def test_get_cache_stats_global(self):
        """Verify statistics calculation for global stats."""
        mock_conn = AsyncMock()
        mock_row = {
            "total_entries": 10,
            "total_hits": 25,
            "avg_similarity": 0.95,
        }
        mock_conn.fetchrow = AsyncMock(return_value=mock_row)

        # Setup async context manager mock
        mock_get_cm = AsyncMock()
        mock_get_cm.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_get_cm.__aexit__ = AsyncMock(return_value=False)
        mock_get = MagicMock(return_value=mock_get_cm)

        with patch("mcp_server.cache.Database.get_connection", mock_get):
            stats = await get_cache_stats()

            assert isinstance(stats, dict)
            assert stats["total_entries"] == 10
            assert stats["total_hits"] == 25
            assert stats["avg_similarity"] == 0.95
            mock_get.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_cache_stats_tenant(self):
        """Verify statistics calculation for tenant-specific stats."""
        mock_conn = AsyncMock()
        mock_row = {
            "total_entries": 5,
            "total_hits": 12,
            "avg_similarity": 0.96,
        }
        mock_conn.fetchrow = AsyncMock(return_value=mock_row)

        # Setup async context manager mock
        mock_get_cm = AsyncMock()
        mock_get_cm.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_get_cm.__aexit__ = AsyncMock(return_value=False)
        mock_get = MagicMock(return_value=mock_get_cm)

        with patch("mcp_server.cache.Database.get_connection", mock_get):
            stats = await get_cache_stats(tenant_id=1)

            assert isinstance(stats, dict)
            assert stats["total_entries"] == 5
            assert stats["total_hits"] == 12
            assert stats["avg_similarity"] == 0.96
            mock_get.assert_called_once()
            # Check both positional and keyword args
            call_args = mock_get.call_args
            assert call_args[0] == (1,) or call_args[1].get("tenant_id") == 1

    @pytest.mark.asyncio
    async def test_get_cache_stats_empty(self):
        """Verify statistics handle empty cache."""
        mock_conn = AsyncMock()
        mock_row = {
            "total_entries": None,
            "total_hits": None,
            "avg_similarity": None,
        }
        mock_conn.fetchrow = AsyncMock(return_value=mock_row)

        # Setup async context manager mock
        mock_get_cm = AsyncMock()
        mock_get_cm.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_get_cm.__aexit__ = AsyncMock(return_value=False)
        mock_get = MagicMock(return_value=mock_get_cm)

        with patch("mcp_server.cache.Database.get_connection", mock_get):
            stats = await get_cache_stats()

            assert stats["total_entries"] == 0
            assert stats["total_hits"] == 0
            assert stats["avg_similarity"] == 0.0


class TestCacheConstants:
    """Test cache configuration constants."""

    def test_similarity_threshold(self):
        """Verify similarity threshold is set correctly."""
        assert SIMILARITY_THRESHOLD == 0.95
        assert 0.0 <= SIMILARITY_THRESHOLD <= 1.0
