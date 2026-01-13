import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from agent_core.nodes.generate import get_few_shot_examples


@pytest.mark.asyncio
@patch("agent_core.nodes.generate.telemetry.update_current_trace")
@patch("agent_core.tools.get_mcp_tools")
async def test_recommendation_telemetry_success(mock_get_mcp_tools, mock_update_trace):
    """Test telemetry emission for successful recommendation."""
    # Setup mocks
    mock_tool = MagicMock()
    mock_tool.name = "recommend_examples"

    reco_data = {
        "examples": [
            {
                "question": "Q1",
                "sql": "S1",
                "source": "approved",
                "canonical_group_id": "F1",
                "metadata": {"status": "verified"},
            },
            {
                "question": "Q2",
                "sql": "S2",
                "source": "seeded",
                "canonical_group_id": "S1",
                "metadata": {"status": "seeded"},
            },
        ],
        "fallback_used": False,
        "metadata": {
            "count_total": 2,
            "count_approved": 1,
            "count_seeded": 1,
            "count_fallback": 0,
            "fingerprints": ["F1", "S1"],
            "sources": ["approved", "seeded"],
            "statuses": ["verified", "seeded"],
            "positions": [0, 1],
            "truncated": False,
        },
    }

    mock_tool.ainvoke = AsyncMock(return_value=json.dumps(reco_data))
    mock_get_mcp_tools.return_value = [mock_tool]

    # Execute
    await get_few_shot_examples("test query", tenant_id=1)

    # Verify
    mock_update_trace.assert_called_once()
    _, kwargs = mock_update_trace.call_args
    metadata = kwargs["metadata"]

    assert metadata["recommendation.used"] is True
    assert metadata["recommendation.fallback_used"] is False
    assert metadata["recommendation.truncated"] is False
    assert metadata["recommendation.count.total"] == 2
    assert metadata["recommendation.count.verified"] == 1
    assert metadata["recommendation.count.seeded"] == 1
    assert metadata["recommendation.count.fallback"] == 0
    assert json.loads(metadata["recommendation.selected.fingerprints"]) == ["F1", "S1"]
    assert json.loads(metadata["recommendation.selected.sources"]) == ["approved", "seeded"]
    assert json.loads(metadata["recommendation.selected.statuses"]) == ["verified", "seeded"]
    assert json.loads(metadata["recommendation.selected.positions"]) == [0, 1]


@pytest.mark.asyncio
@patch("agent_core.nodes.generate.telemetry.update_current_trace")
@patch("agent_core.tools.get_mcp_tools")
async def test_recommendation_telemetry_fallback(mock_get_mcp_tools, mock_update_trace):
    """Test telemetry emission when fallback is used."""
    # Setup mocks
    mock_tool = MagicMock()
    mock_tool.name = "recommend_examples"

    reco_data = {
        "examples": [
            {
                "question": "Q1",
                "sql": "S1",
                "source": "fallback",
                "canonical_group_id": "F4",
                "metadata": {"status": "unverified"},
            }
        ],
        "fallback_used": True,
        "metadata": {
            "count_total": 1,
            "count_approved": 0,
            "count_seeded": 0,
            "count_fallback": 1,
            "fingerprints": ["F4"],
            "sources": ["fallback"],
            "statuses": ["unverified"],
            "positions": [0],
            "truncated": False,
        },
    }

    mock_tool.ainvoke = AsyncMock(return_value=json.dumps(reco_data))
    mock_get_mcp_tools.return_value = [mock_tool]

    # Execute
    await get_few_shot_examples("test query", tenant_id=1)

    # Verify
    mock_update_trace.assert_called_once()
    _, kwargs = mock_update_trace.call_args
    metadata = kwargs["metadata"]

    assert metadata["recommendation.fallback_used"] is True
    assert metadata["recommendation.count.fallback"] == 1


@pytest.mark.asyncio
@patch("agent_core.nodes.generate.telemetry.update_current_trace")
@patch("agent_core.tools.get_mcp_tools")
async def test_recommendation_telemetry_truncated(mock_get_mcp_tools, mock_update_trace):
    """Test telemetry emission when results are truncated."""
    # Setup mocks
    mock_tool = MagicMock()
    mock_tool.name = "recommend_examples"

    reco_data = {
        "examples": [
            {"question": "Q", "sql": "S", "source": "approved", "canonical_group_id": "F"}
        ],
        "fallback_used": False,
        "metadata": {"truncated": True, "count_total": 1},
    }

    mock_tool.ainvoke = AsyncMock(return_value=json.dumps(reco_data))
    mock_get_mcp_tools.return_value = [mock_tool]

    # Execute
    await get_few_shot_examples("test query", tenant_id=1)

    # Verify
    _, kwargs = mock_update_trace.call_args
    metadata = kwargs["metadata"]
    assert metadata["recommendation.truncated"] is True


@pytest.mark.asyncio
@patch("agent_core.nodes.generate.telemetry.update_current_trace")
@patch("agent_core.tools.get_mcp_tools")
async def test_recommendation_telemetry_empty(mock_get_mcp_tools, mock_update_trace):
    """Test telemetry emission for empty results."""
    # Setup mocks
    mock_tool = MagicMock()
    mock_tool.name = "recommend_examples"

    reco_data = {
        "examples": [],
        "fallback_used": False,
        "metadata": {
            "count_total": 0,
            "fingerprints": [],
            "sources": [],
            "statuses": [],
            "positions": [],
        },
    }

    mock_tool.ainvoke = AsyncMock(return_value=json.dumps(reco_data))
    mock_get_mcp_tools.return_value = [mock_tool]

    # Execute
    await get_few_shot_examples("test query", tenant_id=1)

    # Verify
    _, kwargs = mock_update_trace.call_args
    metadata = kwargs["metadata"]
    assert metadata["recommendation.count.total"] == 0
    assert json.loads(metadata["recommendation.selected.fingerprints"]) == []


@pytest.mark.asyncio
@patch("agent_core.nodes.generate.telemetry.update_current_trace")
@patch("agent_core.tools.get_mcp_tools")
async def test_recommendation_telemetry_legacy_tool(mock_get_mcp_tools, mock_update_trace):
    """Test telemetry emission when legacy tool is used (no metadata)."""
    # Setup mocks
    mock_tool = MagicMock()
    mock_tool.name = "get_few_shot_examples"

    # Legacy tool returns a flat list of dicts
    legacy_data = [
        {"question": "Q1", "sql": "S1"},
    ]

    mock_tool.ainvoke = AsyncMock(return_value=json.dumps(legacy_data))
    mock_get_mcp_tools.return_value = [mock_tool]

    # Execute
    await get_few_shot_examples("test query", tenant_id=1)

    # Verify - Metadata should have defaults
    _, kwargs = mock_update_trace.call_args
    metadata = kwargs["metadata"]
    assert metadata["recommendation.used"] is True
    assert metadata["recommendation.count.total"] == 0  # Metadata missing in legacy
    assert json.loads(metadata["recommendation.selected.fingerprints"]) == []
