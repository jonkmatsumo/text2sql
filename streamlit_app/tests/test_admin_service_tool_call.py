"""Unit tests for AdminService tool calls."""

import logging
import sys
from types import ModuleType
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from streamlit_app.service.admin import AdminService

# ExceptionGroup is a Python 3.11+ built-in
# flake8: noqa: F821


@pytest.fixture
def mock_agent_core():
    """Mock agent_core module for tests that call _call_tool directly."""
    # Create mock modules
    mock_tools_module = ModuleType("agent_core.tools")
    mock_agent_core_module = ModuleType("agent_core")

    # Create mock mcp_tools_context as an async context manager
    mock_mcp_tools_context = MagicMock()
    mock_get_mcp_tools = AsyncMock()

    # Set up the async context manager
    mock_mcp_tools_context.return_value.__aenter__ = mock_get_mcp_tools
    mock_mcp_tools_context.return_value.__aexit__ = AsyncMock(return_value=None)

    # Create unpack_mcp_result mock
    mock_unpack = MagicMock(side_effect=lambda x: x)  # Pass through

    mock_tools_module.mcp_tools_context = mock_mcp_tools_context
    mock_tools_module.unpack_mcp_result = mock_unpack

    # Install in sys.modules
    original_agent_core = sys.modules.get("agent_core")
    original_tools = sys.modules.get("agent_core.tools")

    sys.modules["agent_core"] = mock_agent_core_module
    sys.modules["agent_core.tools"] = mock_tools_module

    yield mock_get_mcp_tools

    # Restore original modules
    if original_agent_core is not None:
        sys.modules["agent_core"] = original_agent_core
    else:
        sys.modules.pop("agent_core", None)

    if original_tools is not None:
        sys.modules["agent_core.tools"] = original_tools
    else:
        sys.modules.pop("agent_core.tools", None)


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


class TestCallToolExceptionHandling:
    """Tests for _call_tool ExceptionGroup handling."""

    @pytest.mark.asyncio
    async def test_single_exception_group_unwrapped(self, mock_agent_core, caplog):
        """Verify single-exception group surfaces the root cause message."""
        root_cause = ValueError("root cause error")
        exc_group = ExceptionGroup("TaskGroup", [root_cause])

        mock_tool = MagicMock()
        mock_tool.name = "test_tool"
        mock_tool.ainvoke = AsyncMock(side_effect=exc_group)

        mock_agent_core.return_value = [mock_tool]

        with caplog.at_level(logging.ERROR):
            result = await AdminService._call_tool("test_tool", {"arg": "val"})

        # Verify unwrapped error message is surfaced
        assert result == {"error": "root cause error"}
        # Verify traceback was logged
        assert "ExceptionGroup with single root cause" in caplog.text

    @pytest.mark.asyncio
    async def test_multi_exception_group_preserved(self, mock_agent_core):
        """Verify multi-exception group preserves original str representation."""
        exc_group = ExceptionGroup("TaskGroup", [ValueError("a"), RuntimeError("b")])

        mock_tool = MagicMock()
        mock_tool.name = "test_tool"
        mock_tool.ainvoke = AsyncMock(side_effect=exc_group)

        mock_agent_core.return_value = [mock_tool]

        result = await AdminService._call_tool("test_tool", {})

        # Multi-exception groups are not unwrapped
        assert "error" in result
        assert "TaskGroup" in result["error"]

    @pytest.mark.asyncio
    async def test_regular_exception_unchanged(self, mock_agent_core):
        """Verify regular exceptions are handled normally."""
        mock_tool = MagicMock()
        mock_tool.name = "test_tool"
        mock_tool.ainvoke = AsyncMock(side_effect=RuntimeError("regular error"))

        mock_agent_core.return_value = [mock_tool]

        result = await AdminService._call_tool("test_tool", {})

        assert result == {"error": "regular error"}

    @pytest.mark.asyncio
    async def test_successful_call_unchanged(self, mock_agent_core):
        """Verify successful tool calls are not affected."""
        mock_tool = MagicMock()
        mock_tool.name = "test_tool"
        mock_tool.ainvoke = AsyncMock(return_value={"success": True, "data": "value"})

        mock_agent_core.return_value = [mock_tool]

        result = await AdminService._call_tool("test_tool", {})

        assert result == {"success": True, "data": "value"}
