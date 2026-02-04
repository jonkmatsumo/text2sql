from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dal.database import Database
from dal.query_result import QueryResult


class TestDatabaseFetchQuery:
    """Tests for Database.fetch_query behavior."""

    @pytest.mark.asyncio
    async def test_fetch_query_default_rows_only(self):
        """Default path returns rows without columns."""
        mock_conn = AsyncMock()
        mock_conn.fetch = AsyncMock(return_value=[{"id": 1}])
        mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_conn.__aexit__ = AsyncMock(return_value=False)
        mock_get = MagicMock(return_value=mock_conn)

        with patch("dal.database.Database.get_connection", mock_get):
            result = await Database.fetch_query("SELECT 1", tenant_id=1, include_columns=False)

        assert isinstance(result, QueryResult)
        assert result.rows == [{"id": 1}]
        assert result.columns is None

    @pytest.mark.asyncio
    async def test_fetch_query_include_columns_without_support(self):
        """Include-columns falls back to rows when unsupported."""
        mock_conn = AsyncMock()
        mock_conn.fetch = AsyncMock(return_value=[{"id": 1}])
        mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_conn.__aexit__ = AsyncMock(return_value=False)
        mock_get = MagicMock(return_value=mock_conn)

        with patch("dal.database.Database.get_connection", mock_get):
            result = await Database.fetch_query("SELECT 1", tenant_id=1, include_columns=True)

        assert result.rows == [{"id": 1}]
        assert result.columns is None
