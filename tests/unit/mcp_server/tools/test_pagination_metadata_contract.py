"""Contract tests for canonical pagination/truncation metadata on list tools."""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, patch

import pytest

from mcp_server.tools.get_table_schema import handler as get_table_schema_handler
from mcp_server.tools.list_tables import handler as list_tables_handler
from mcp_server.utils.tracing import trace_tool


def _assert_canonical_metadata(payload: dict) -> None:
    metadata = payload["metadata"]
    assert "items_returned" in metadata
    assert "limit_applied" in metadata
    assert "is_truncated" in metadata
    assert "truncation_reason" in metadata
    assert "next_page_token" in metadata
    assert "next_cursor" in metadata
    assert metadata["next_page_token"] == metadata["next_cursor"]


@pytest.mark.asyncio
async def test_list_tables_emits_canonical_metadata_fields():
    """list_tables should emit canonical pagination/truncation metadata keys."""
    mock_store = AsyncMock()
    mock_store.list_tables.return_value = ["users", "orders"]

    with patch("dal.database.Database.get_metadata_store", return_value=mock_store):
        traced_handler = trace_tool("list_tables")(list_tables_handler)
        payload = json.loads(await traced_handler(tenant_id=1))

    _assert_canonical_metadata(payload)
    assert payload["metadata"]["items_returned"] == len(payload["result"])
    assert payload["metadata"]["is_truncated"] is False


@pytest.mark.asyncio
async def test_get_table_schema_emits_canonical_metadata_fields():
    """get_table_schema should emit canonical pagination/truncation metadata keys."""
    mock_store = AsyncMock()
    mock_store.get_table_definition.return_value = json.dumps(
        {"table_name": "users", "columns": [], "foreign_keys": []}
    )

    with patch("dal.database.Database.get_metadata_store", return_value=mock_store):
        traced_handler = trace_tool("get_table_schema")(get_table_schema_handler)
        payload = json.loads(await traced_handler(table_names=["users"], tenant_id=1))

    _assert_canonical_metadata(payload)
    assert payload["metadata"]["items_returned"] == len(payload["result"])
    assert payload["metadata"]["is_truncated"] is False
