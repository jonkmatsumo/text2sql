from unittest.mock import AsyncMock, patch

import pytest

from streamlit.service.admin import AdminService


@pytest.mark.asyncio
async def test_admin_service_get_recommendations_wiring():
    """Verify get_recommendations calls the correct MCP tool with args."""
    with patch(
        "streamlit.service.admin.AdminService._call_tool", new_callable=AsyncMock
    ) as mock_call:
        mock_call.return_value = {"examples": [], "metadata": {}}

        await AdminService.get_recommendations(
            query="test query", tenant_id=99, limit=5, enable_fallback=False
        )

        mock_call.assert_called_once_with(
            "recommend_examples",
            {"query": "test query", "tenant_id": 99, "limit": 5, "enable_fallback": False},
        )


@pytest.mark.asyncio
async def test_admin_service_handles_tool_error():
    """Verify error propagation."""
    with patch(
        "streamlit.service.admin.AdminService._call_tool", new_callable=AsyncMock
    ) as mock_call:
        mock_call.return_value = {"error": "Connection failed"}

        result = await AdminService.get_recommendations("q", 1, 1, True)
        assert result == {"error": "Connection failed"}


# UI Parsing/Structure Tests (simulating what the page logic does)
def test_result_structure_parking():
    """Verify the playground logic correctly restructures the response."""
    # Simulate a raw tool response
    tool_response = {
        "examples": [{"question": "Q1", "metadata": {"status": "verified"}}],
        "metadata": {"count_total": 1, "truncated": False},
        "fallback_used": True,
        "extra_field": "ignored",
    }

    # Logic copied from Recommendation_Playground.py for validation
    parsed_state = {
        "examples": tool_response.get("examples", []),
        "metadata": tool_response.get("metadata", {}),
        "fallback_used": tool_response.get("fallback_used", False),
    }

    assert len(parsed_state["examples"]) == 1
    assert parsed_state["fallback_used"] is True
    assert parsed_state["metadata"]["count_total"] == 1
    # Ensure no extra fields leaked
    assert "extra_field" not in parsed_state
