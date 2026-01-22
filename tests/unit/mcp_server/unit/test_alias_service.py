"""Unit tests for CanonicalAliasService."""

from unittest.mock import AsyncMock, patch

import pytest


class TestCanonicalAliasService:
    """Tests for the CanonicalAliasService."""

    @pytest.fixture(autouse=True)
    def reset_cache(self):
        """Reset the service cache before each test."""
        from mcp_server.services.canonicalization.alias_service import CanonicalAliasService

        CanonicalAliasService.invalidate_cache()
        yield
        CanonicalAliasService.invalidate_cache()

    @pytest.mark.asyncio
    async def test_load_aliases_populates_cache(self):
        """Test that load_aliases correctly populates the cache from DB."""
        from mcp_server.services.canonicalization.alias_service import CanonicalAliasService

        # Mock database rows
        mock_rows = [
            {"id": "users", "label": "TABLE", "pattern": "customers"},
            {"id": "users", "label": "TABLE", "pattern": "buyers"},
            {"id": "users", "label": "TABLE", "pattern": "users"},  # Same as ID
            {"id": "orders", "label": "TABLE", "pattern": "purchases"},
            {"id": "users.email", "label": "COLUMN", "pattern": "customer_email"},
        ]

        mock_conn = AsyncMock()
        mock_conn.fetch = AsyncMock(return_value=mock_rows)

        mock_cm = AsyncMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_cm.__aexit__ = AsyncMock(return_value=None)

        with patch(
            "mcp_server.services.canonicalization.alias_service.Database.get_connection",
            return_value=mock_cm,
        ):
            await CanonicalAliasService.load_aliases()

            # Check table aliases
            users_aliases = await CanonicalAliasService.get_aliases_for_table("users")
            assert "customers" in users_aliases
            assert "buyers" in users_aliases
            # Should not include "users" since it matches the ID
            assert "users" not in users_aliases

            orders_aliases = await CanonicalAliasService.get_aliases_for_table("orders")
            assert "purchases" in orders_aliases

            # Check column aliases
            email_aliases = await CanonicalAliasService.get_aliases_for_column("users.email")
            assert "customer_email" in email_aliases

    @pytest.mark.asyncio
    async def test_get_aliases_returns_empty_for_unknown_table(self):
        """Test that unknown tables return empty alias list."""
        from mcp_server.services.canonicalization.alias_service import CanonicalAliasService

        mock_conn = AsyncMock()
        mock_conn.fetch = AsyncMock(return_value=[])

        mock_cm = AsyncMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_cm.__aexit__ = AsyncMock(return_value=None)

        with patch(
            "mcp_server.services.canonicalization.alias_service.Database.get_connection",
            return_value=mock_cm,
        ):
            await CanonicalAliasService.load_aliases()

            aliases = await CanonicalAliasService.get_aliases_for_table("nonexistent_table")
            assert aliases == []

    @pytest.mark.asyncio
    async def test_invalidate_cache_clears_loaded_flag(self):
        """Test that invalidate_cache allows reloading."""
        from mcp_server.services.canonicalization.alias_service import CanonicalAliasService

        # Simulate a loaded state
        CanonicalAliasService._loaded = True
        CanonicalAliasService._cache = {"TABLE": {"users": ["customers"]}}

        CanonicalAliasService.invalidate_cache()

        assert not CanonicalAliasService._loaded
        assert CanonicalAliasService._cache == {}
