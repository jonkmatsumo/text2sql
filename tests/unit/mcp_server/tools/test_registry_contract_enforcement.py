"""Tests for registry-level MCP envelope contract enforcement."""

from __future__ import annotations

import json

import pytest

from common.models.tool_envelopes import (
    ExecuteSQLQueryMetadata,
    ExecuteSQLQueryResponseEnvelope,
    ToolResponseEnvelope,
)
from mcp_server.tools.registry import get_all_tool_names
from mcp_server.utils.contract_enforcement import enforce_tool_response_contract


@pytest.mark.asyncio
async def test_malformed_raw_tool_output_becomes_typed_malformed_envelope():
    """Raw non-envelope responses should be converted to typed malformed errors."""

    async def _raw_handler():
        return "totally-unstructured-response"

    wrapped = enforce_tool_response_contract("dummy_tool")(_raw_handler)
    result = await wrapped()

    payload = json.loads(result)
    assert payload["error"]["category"] == "tool_response_malformed"
    assert payload["error"]["code"] == "TOOL_RESPONSE_MALFORMED"
    assert payload["metadata"]["provider"] == "dummy_tool"
    assert "totally-unstructured-response" not in result


@pytest.mark.asyncio
@pytest.mark.parametrize("tool_name", get_all_tool_names())
async def test_registered_tools_contract_wrapper_accepts_happy_path_envelopes(tool_name: str):
    """All registered tool names should pass contract validation for a mocked happy-path payload."""
    if tool_name == "execute_sql_query":
        response = ExecuteSQLQueryResponseEnvelope(
            rows=[{"id": 1}],
            metadata=ExecuteSQLQueryMetadata(rows_returned=1, is_truncated=False),
        ).model_dump_json(exclude_none=True)
    else:
        response = ToolResponseEnvelope(
            result={"ok": True, "tool": tool_name},
        ).model_dump_json(exclude_none=True)

    async def _happy_handler():
        return response

    wrapped = enforce_tool_response_contract(tool_name)(_happy_handler)
    result = await wrapped()
    payload = json.loads(result)

    # The wrapper should leave valid envelopes intact and parseable.
    if tool_name == "execute_sql_query":
        ExecuteSQLQueryResponseEnvelope.model_validate(payload)
    else:
        ToolResponseEnvelope.model_validate(payload)
