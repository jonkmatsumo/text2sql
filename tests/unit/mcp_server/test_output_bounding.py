"""Unit tests for tool output bounding."""

import json
from unittest.mock import AsyncMock, patch

import pytest

from mcp_server.tools.get_table_schema import handler as get_table_schema_handler
from mcp_server.tools.list_tables import handler as list_tables_handler


@pytest.mark.asyncio
async def test_list_tables_truncation():
    """Verify that list_tables output is truncated when it exceeds the limit."""
    # Create a large list of tables
    large_tables = [f"table_{i}" for i in range(1000)]

    mock_store = AsyncMock()
    mock_store.list_tables.return_value = large_tables

    # We want to force a small limit for testing
    with patch("dal.database.Database.get_metadata_store", return_value=mock_store):
        with patch("mcp_server.utils.tracing.get_env_str", return_value="warn"):
            # Set a very small payload limit (e.g., 500 bytes)
            with patch("mcp_server.utils.tool_output.get_env_int", return_value=500):
                # We need to call it through the registry/wrapped logic to test the wrapper
                from mcp_server.utils.tracing import trace_tool

                traced_handler = trace_tool("list_tables")(list_tables_handler)

                result_str = await traced_handler(tenant_id=1)
                result = json.loads(result_str)

                # Verify truncation
                assert len(result["result"]) < 1000
                assert result["metadata"]["provider"] == "postgres"
                assert result["metadata"]["is_truncated"] is True
                assert result["metadata"]["truncated"] is True
                assert result["metadata"]["items_total"] == 1000
                assert result["metadata"]["items_returned"] == len(result["result"])
                assert result["metadata"]["returned_count"] == len(result["result"])
                assert result["metadata"]["limit_applied"] == len(result["result"])
                assert result["metadata"]["bytes_total"] >= result["metadata"]["bytes_returned"]
                assert result["metadata"]["truncation_reason"] in {
                    "max_items",
                    "max_string_bytes",
                    "max_bytes",
                }

                # Check that we can find the truncation evidence if we parse the span
                # (but here we just check the resulting JSON)


@pytest.mark.asyncio
async def test_get_table_schema_truncation_metadata():
    """Verify get_table_schema output is bounded with truncation metadata."""
    table_names = [f"table_{i}" for i in range(80)]

    mock_store = AsyncMock()
    mock_store.get_table_definition.side_effect = lambda table: json.dumps(
        {
            "table_name": table,
            "columns": [
                {"name": f"col_{i}", "type": "text", "description": "x" * 200} for i in range(20)
            ],
            "foreign_keys": [],
        }
    )

    with patch("dal.database.Database.get_metadata_store", return_value=mock_store):
        with patch("mcp_server.utils.tracing.get_env_str", return_value="warn"):
            with patch("mcp_server.utils.tool_output.get_env_int", return_value=1200):
                from mcp_server.utils.tracing import trace_tool

                traced_handler = trace_tool("get_table_schema")(get_table_schema_handler)
                result_str = await traced_handler(table_names=table_names, tenant_id=1)
                result = json.loads(result_str)

                assert result["metadata"]["is_truncated"] is True
                assert result["metadata"]["truncated"] is True
                assert result["metadata"]["items_total"] == len(table_names)
                assert result["metadata"]["items_returned"] == len(result["result"])
                assert result["metadata"]["returned_count"] == len(result["result"])
                assert result["metadata"]["limit_applied"] == len(result["result"])
                assert result["metadata"]["items_returned"] < len(table_names)
                assert result["metadata"]["bytes_total"] >= result["metadata"]["bytes_returned"]
                assert isinstance(result["result"], list)


@pytest.mark.asyncio
async def test_bound_tool_output_dict_truncation():
    """Test the low-level bounding utility directly for dicts."""
    from mcp_server.utils.tool_output import bound_tool_output

    data = {"result": [{"id": i, "data": "x" * 100} for i in range(10)], "metadata": {"foo": "bar"}}

    # Limit to 300 bytes (should only fit 2-3 items)
    bounded, meta = bound_tool_output(data, max_bytes=300)

    assert meta["truncated"] is True
    assert len(bounded["result"]) < 10
    assert "foo" in bounded["metadata"]
