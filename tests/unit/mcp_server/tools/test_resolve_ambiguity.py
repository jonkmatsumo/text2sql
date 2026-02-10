import json
from unittest.mock import MagicMock, patch

import pytest

from mcp_server.tools.resolve_ambiguity import TOOL_NAME, handler


class TestResolveAmbiguity:
    """Unit tests for resolve_ambiguity tool."""

    def test_tool_name_no_suffix(self):
        """Verify TOOL_NAME does not end with '_tool'."""
        assert not TOOL_NAME.endswith("_tool")
        assert TOOL_NAME == "resolve_ambiguity"

    @pytest.mark.asyncio
    async def test_resolve_ambiguity_oversized_query_rejected(self):
        """Oversized query should be rejected with structured error."""
        oversized_query = "x" * ((10 * 1024) + 1)
        result_json = await handler(query=oversized_query, schema_context=[])
        data = json.loads(result_json)
        assert data["error"]["category"] == "invalid_request"
        assert data["error"]["sql_state"] == "INPUT_TOO_LARGE"

    @pytest.mark.asyncio
    async def test_resolve_ambiguity_schema_context_too_large_rejected(self):
        """Excessive schema_context names should be rejected deterministically."""
        schema_context = [{"name": f"table_{i}"} for i in range(101)]
        result_json = await handler(query="show orders", schema_context=schema_context)
        data = json.loads(result_json)
        assert data["error"]["category"] == "invalid_request"
        assert data["error"]["sql_state"] == "TOO_MANY_ITEMS"

    @pytest.mark.asyncio
    async def test_resolve_ambiguity_success(self):
        """Valid payload should call resolver and return envelope."""
        mock_resolver = MagicMock()
        mock_resolver.resolve.return_value = {
            "status": "CLEAR",
            "resolved_bindings": {},
            "ambiguities": [],
        }

        with patch(
            "mcp_server.tools.resolve_ambiguity.get_resolver",
            return_value=mock_resolver,
        ):
            result_json = await handler(
                query="show customers",
                schema_context=[{"name": "customers", "type": "Table"}],
            )

        data = json.loads(result_json)
        assert data["result"]["status"] == "CLEAR"
