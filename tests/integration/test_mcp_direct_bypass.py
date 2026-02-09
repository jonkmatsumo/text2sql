"""Integration tests for direct MCP bypass vectors.

These tests verify that security invariants are enforced at the MCP tool handler level,
even if the Agent's own validation layers are bypassed or compromised.
"""

import json
from unittest.mock import patch

import pytest

from mcp_server.tools.execute_sql_query import handler as execute_handler
from mcp_server.tools.get_sample_data import handler as sample_handler


@pytest.mark.asyncio
async def test_direct_bypass_sql_length():
    """Verify that direct MCP calls still enforce SQL length limits."""
    # Force a very small limit for testing
    with patch("mcp_server.tools.execute_sql_query.get_env_int", return_value=5):
        result_json = await execute_handler(sql_query="SELECT 123", tenant_id=1)
        result = json.loads(result_json)

        assert "error" in result
        err = result["error"]
        msg = err["message"] if isinstance(err, dict) else err
        assert "exceeds maximum length" in msg


@pytest.mark.asyncio
async def test_direct_bypass_blocked_functions():
    """Verify that direct MCP calls still block dangerous functions."""
    # Call handler directly with a blocked function
    result_json = await execute_handler(sql_query="SELECT pg_sleep(1)", tenant_id=1)
    result = json.loads(result_json)

    assert "error" in result
    err = result["error"]
    msg = err["message"] if isinstance(err, dict) else err
    assert "Forbidden function" in msg
    assert "PG_SLEEP" in msg


@pytest.mark.asyncio
async def test_direct_bypass_tenant_isolation():
    """Verify that direct MCP calls still require tenant_id."""
    # Call execute_handler with None tenant_id
    result_json = await execute_handler(sql_query="SELECT 1", tenant_id=None)
    result = json.loads(result_json)

    assert "error" in result
    err = result["error"]
    msg = err["message"] if isinstance(err, dict) else err
    assert "Tenant ID is required" in msg


@pytest.mark.asyncio
async def test_direct_bypass_limit_bounds():
    """Verify that direct MCP calls still enforce limit bounds in get_sample_data."""
    # Call sample_handler with excessive limit
    result_json = await sample_handler(table_name="users", tenant_id=1, limit=5000)
    result = json.loads(result_json)

    # Verify rejection (some tools use 'error' as dict, others as string)
    assert "error" in result
    err = result["error"]
    msg = err["message"] if isinstance(err, dict) else err
    assert "Must be between" in msg
