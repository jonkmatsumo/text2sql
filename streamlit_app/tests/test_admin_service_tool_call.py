"""Unit tests for AdminService tool calls."""

from unittest.mock import AsyncMock, patch

import pytest

from streamlit_app.service.admin import AdminService


@pytest.mark.asyncio
async def test_list_pin_rules_calls_tool():
    """Verify list_pin_rules calls the correct MCP tool."""
    with patch.object(AdminService, "_call_tool", new_callable=AsyncMock) as mock_call:
        mock_call.return_value = [
            {
                "id": "123",
                "tenant_id": 1,
                "match_type": "exact",
                "match_value": "foo",
                "registry_example_ids": [],
                "priority": 10,
                "enabled": True,
                "created_at": None,
                "updated_at": None,
            }
        ]

        rules = await AdminService.list_pin_rules(tenant_id=1)

        mock_call.assert_called_once_with("manage_pin_rules", {"operation": "list", "tenant_id": 1})
        assert len(rules) == 1
        assert rules[0].match_value == "foo"
        assert rules[0].enabled is True


@pytest.mark.asyncio
async def test_upsert_pin_rule_calls_tool():
    """Verify upsert_pin_rule calls the correct MCP tool."""
    with patch.object(AdminService, "_call_tool", new_callable=AsyncMock) as mock_call:
        mock_call.return_value = {
            "id": "456",
            "tenant_id": 1,
            "match_type": "contains",
            "match_value": "bar",
            "registry_example_ids": [],
            "priority": 5,
            "enabled": True,
            "created_at": None,
            "updated_at": None,
        }

        rule = await AdminService.upsert_pin_rule(
            tenant_id=1, match_type="contains", match_value="bar"
        )

        mock_call.assert_called_once()
        args = mock_call.call_args[0]
        assert args[0] == "manage_pin_rules"
        assert args[1]["operation"] == "upsert"
        assert args[1]["match_value"] == "bar"

        assert rule.id == "456"


@pytest.mark.asyncio
async def test_delete_pin_rule_calls_tool():
    """Verify delete_pin_rule calls the correct MCP tool."""
    with patch.object(AdminService, "_call_tool", new_callable=AsyncMock) as mock_call:
        mock_call.return_value = {"success": True}

        success = await AdminService.delete_pin_rule(rule_id="789", tenant_id=1)

        mock_call.assert_called_once_with(
            "manage_pin_rules", {"operation": "delete", "rule_id": "789", "tenant_id": 1}
        )
        assert success is True
