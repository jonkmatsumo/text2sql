import json
from unittest.mock import AsyncMock, patch

import pytest

from mcp_server.tools.interaction.update_interaction import TOOL_NAME, handler


class TestUpdateInteraction:
    """Tests for update_interaction tool."""

    def test_tool_name_no_suffix(self):
        """Verify TOOL_NAME does not end with '_tool'."""
        assert not TOOL_NAME.endswith("_tool")
        assert TOOL_NAME == "update_interaction"

    @pytest.mark.asyncio
    async def test_update_interaction_success(self):
        """Test update_interaction updates successfully."""
        with patch(
            "mcp_server.tools.interaction.update_interaction.get_interaction_store"
        ) as mock_get_store:
            mock_store = AsyncMock()
            mock_store.update_interaction_result = AsyncMock()
            mock_get_store.return_value = mock_store

            response_json = await handler(
                interaction_id="int-1",
                tenant_id=1,
                generated_sql="SELECT * FROM users",
                response_payload='{"rows": []}',
                execution_status="SUCCESS",
                error_type=None,
                tables_used=["users"],
            )
            response = json.loads(response_json)

            assert response["result"] == "OK"
            mock_store.update_interaction_result.assert_called_once()

    @pytest.mark.asyncio
    async def test_update_interaction_failure_status(self):
        """Test update_interaction with failure status."""
        with patch(
            "mcp_server.tools.interaction.update_interaction.get_interaction_store"
        ) as mock_get_store:
            mock_store = AsyncMock()
            mock_store.update_interaction_result = AsyncMock()
            mock_get_store.return_value = mock_store

            response_json = await handler(
                interaction_id="int-1",
                tenant_id=1,
                execution_status="FAILURE",
                error_type="SYNTAX_ERROR",
            )
            response = json.loads(response_json)

            assert response["result"] == "OK"
            call_args = mock_store.update_interaction_result.call_args[0]
            # Args: interaction_id, generated_sql, response_payload,
            #       execution_status, error_type, tables_used
            assert call_args[0] == "int-1"
            assert call_args[1] == 1
            assert call_args[4] == "FAILURE"
            assert call_args[5] == "SYNTAX_ERROR"

    @pytest.mark.asyncio
    async def test_update_interaction_requires_tenant_id(self):
        """Missing tenant_id should be rejected."""
        response_json = await handler(interaction_id="int-1", tenant_id=None)
        response = json.loads(response_json)
        assert response["error"]["sql_state"] == "MISSING_TENANT_ID"

    @pytest.mark.asyncio
    async def test_update_interaction_rejects_cross_tenant_access(self):
        """Tenant mismatch should return deterministic scoped error."""
        with patch(
            "mcp_server.tools.interaction.update_interaction.get_interaction_store"
        ) as mock_get_store:
            mock_store = AsyncMock()
            mock_store.update_interaction_result = AsyncMock(
                side_effect=ValueError("Interaction not found for tenant scope.")
            )
            mock_get_store.return_value = mock_store

            response_json = await handler(interaction_id="int-1", tenant_id=9)
            response = json.loads(response_json)

            assert response["error"]["sql_state"] == "TENANT_SCOPE_VIOLATION"

    @pytest.mark.asyncio
    async def test_update_interaction_rejects_cross_tenant_access_with_payload(self):
        """Cross-tenant update should still be rejected when full payload is provided."""
        with patch(
            "mcp_server.tools.interaction.update_interaction.get_interaction_store"
        ) as mock_get_store:
            mock_store = AsyncMock()
            mock_store.update_interaction_result = AsyncMock(
                side_effect=ValueError("Interaction not found for tenant scope.")
            )
            mock_get_store.return_value = mock_store

            response_json = await handler(
                interaction_id="int-1",
                tenant_id=9,
                generated_sql="SELECT * FROM users",
                response_payload='{"rows":[{"id":1}]}',
                execution_status="FAILURE",
                error_type="PERMISSION_DENIED",
                tables_used=["users"],
            )
            response = json.loads(response_json)

            assert response["error"]["sql_state"] == "TENANT_SCOPE_VIOLATION"
            mock_store.update_interaction_result.assert_awaited_once()
