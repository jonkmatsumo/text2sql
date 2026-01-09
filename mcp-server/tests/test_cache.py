"""Tests for semantic caching."""

from unittest.mock import AsyncMock, patch

import pytest
from mcp_server.models import CacheLookupResult
from mcp_server.services.cache_service import (
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
        mock_store = AsyncMock()
        mock_store.lookup.return_value = None

        mock_embedding = [0.1] * 384

        with patch(
            "mcp_server.services.cache_service.Database.get_cache_store", return_value=mock_store
        ):
            with patch(
                "mcp_server.services.cache_service.RagEngine.embed_text",
                return_value=mock_embedding,
            ):
                cached = await lookup_cache("What is the total revenue?", tenant_id=1)

                assert cached is None
                mock_store.lookup.assert_called_once()
                args, kwargs = mock_store.lookup.call_args
                assert args[0] == mock_embedding
                assert args[1] == 1
                assert kwargs["threshold"] == SIMILARITY_THRESHOLD

    @pytest.mark.asyncio
    async def test_lookup_cache_hit(self):
        """Test that cache returns cached SQL when similarity >= threshold."""
        mock_store = AsyncMock()
        mock_result = CacheLookupResult(
            cache_id="1", value="SELECT SUM(amount) FROM payment;", similarity=0.96
        )
        mock_store.lookup.return_value = mock_result

        mock_embedding = [0.1] * 384

        with patch(
            "mcp_server.services.cache_service.Database.get_cache_store", return_value=mock_store
        ):
            with patch(
                "mcp_server.services.cache_service.RagEngine.embed_text",
                return_value=mock_embedding,
            ):
                with patch(
                    "mcp_server.services.cache_service.update_cache_access", new_callable=AsyncMock
                ) as mock_update:
                    cached = await lookup_cache("What is the total revenue?", tenant_id=1)

                    assert cached == "SELECT SUM(amount) FROM payment;"
                    mock_store.lookup.assert_called_once()

                    # Verify non-blocking update call via asyncio.create_task
                    # Note: asyncio.create_task schedules it, we can't easily wait here
                    # in unit test standardly
                    # but we mocked update_cache_access directly, so we can verify call
                    mock_update.assert_called_once_with("1", 1)

    @pytest.mark.asyncio
    async def test_lookup_cache_uses_cache_store(self):
        """Verify database connection uses cache store pattern."""
        mock_store = AsyncMock()
        mock_store.lookup.return_value = None
        mock_embedding = [0.1] * 384

        with patch(
            "mcp_server.services.cache_service.Database.get_cache_store", return_value=mock_store
        ) as mock_get:
            with patch(
                "mcp_server.services.cache_service.RagEngine.embed_text",
                return_value=mock_embedding,
            ):
                await lookup_cache("test query", tenant_id=1)

                mock_get.assert_called_once()
                mock_store.lookup.assert_called_once()


class TestUpdateCache:
    """Unit tests for update_cache function."""

    @pytest.mark.asyncio
    async def test_update_cache(self):
        """Verify cache insertion delegates to store."""
        mock_store = AsyncMock()
        mock_embedding = [0.1] * 384

        with patch(
            "mcp_server.services.cache_service.Database.get_cache_store", return_value=mock_store
        ):
            with patch(
                "mcp_server.services.cache_service.RagEngine.embed_text",
                return_value=mock_embedding,
            ):
                await update_cache(
                    "What is the total revenue?",
                    "SELECT SUM(amount) FROM payment;",
                    tenant_id=1,
                )

                mock_store.store.assert_called_once()
                kwargs = mock_store.store.call_args[1]
                assert kwargs["user_query"] == "What is the total revenue?"
                assert kwargs["generated_sql"] == "SELECT SUM(amount) FROM payment;"
                assert kwargs["tenant_id"] == 1
                assert kwargs["query_embedding"] == mock_embedding


class TestUpdateCacheAccess:
    """Unit tests for update_cache_access function."""

    @pytest.mark.asyncio
    async def test_update_cache_access(self):
        """Verify hit count updates delegate to store."""
        mock_store = AsyncMock()

        with patch(
            "mcp_server.services.cache_service.Database.get_cache_store", return_value=mock_store
        ):
            await update_cache_access(cache_id="1", tenant_id=1)

            mock_store.record_hit.assert_called_once_with("1", 1)


class TestGetCacheStats:
    """Unit tests for get_cache_stats function."""

    # We removed logic from implementation as per DAL strictness,
    # so we test the stub behavior.

    @pytest.mark.asyncio
    async def test_get_cache_stats_stub(self):
        """Verify statistics stub."""
        stats = await get_cache_stats()
        assert stats.get("status") == "Stats not implementing in DAL v1"


class TestCacheConstants:
    """Test cache configuration constants."""

    def test_similarity_threshold(self):
        """Verify similarity threshold is set correctly."""
        assert SIMILARITY_THRESHOLD == 0.95
        assert 0.0 <= SIMILARITY_THRESHOLD <= 1.0
