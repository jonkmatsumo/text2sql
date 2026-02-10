"""Unit tests for get_sample_data tool."""

import json
from unittest.mock import AsyncMock, patch

import pytest

from mcp_server.tools.get_sample_data import handler


@pytest.fixture
def mock_db_connection():
    """Mock database connection."""
    mock_conn = AsyncMock()
    # Mock fetching rows
    mock_conn.fetch.return_value = [{"id": 1, "name": "foo"}, {"id": 2, "name": "bar"}]
    # Async context manager support
    mock_conn.__aenter__.return_value = mock_conn
    mock_conn.__aexit__.return_value = None
    return mock_conn


@pytest.mark.asyncio
async def test_get_sample_data_missing_tenant_id():
    """Verify that calling get_sample_data without tenant_id returns a typed error."""
    result_str = await handler("users", tenant_id=None)
    result = json.loads(result_str)

    assert "error" in result
    assert result["error"]["message"] == "Tenant ID is required for get_sample_data."
    assert result["error"]["category"] == "invalid_request"


@pytest.mark.asyncio
async def test_get_sample_data_with_tenant_id(mock_db_connection):
    """Verify that providing tenant_id uses the tenant-scoped connection."""
    with patch(
        "dal.database.Database.get_connection", return_value=mock_db_connection
    ) as mock_get_conn:
        # Also mock get_query_target_provider to avoid DB call
        with patch("dal.database.Database.get_query_target_provider", return_value="postgres"):
            result_str = await handler("users", tenant_id=123)

            # Check result structure
            result = json.loads(result_str)
            assert "result" in result
            assert len(result["result"]) == 2

            # Verify correct connection call
            mock_get_conn.assert_called_once_with(tenant_id=123)

            # Verify query execution
            mock_db_connection.fetch.assert_called_once()
            args = mock_db_connection.fetch.call_args
            query = args[0][0]
            assert 'SELECT * FROM "users"' in query


@pytest.mark.asyncio
async def test_get_sample_data_invalid_limit():
    """Verify that get_sample_data rejects invalid limits."""
    # Test limit <= 0
    result_str = await handler("users", limit=0, tenant_id=1)
    result = json.loads(result_str)
    assert "error" in result
    assert "Invalid limit" in result["error"]["message"]
    assert result["error"]["category"] == "invalid_request"

    # Test limit > 100
    result_str = await handler("users", limit=101, tenant_id=1)
    result = json.loads(result_str)
    assert "error" in result
    assert "100" in result["error"]["message"]
    assert result["error"]["category"] == "invalid_request"
