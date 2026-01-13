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


@pytest.mark.asyncio
@patch("agent_core.nodes.generate.telemetry.start_span")
@patch("agent_core.nodes.generate.telemetry.update_current_trace")
@patch("agent_core.tools.get_mcp_tools")
async def test_recommendation_telemetry_bounding_items(
    mock_get_mcp_tools, mock_update_trace, mock_start_span
):
    """Test that list items are capped at 10."""
    mock_span = MagicMock()
    mock_start_span.return_value.__enter__.return_value = mock_span
    mock_tool = MagicMock()
    mock_tool.name = "recommend_examples"

    # 15 items
    items = [f"F{i}" for i in range(15)]
    reco_data = {
        "examples": [],
        "metadata": {
            "count_total": 15,
            "fingerprints": items,
            "sources": ["source"] * 15,
            "statuses": ["ok"] * 15,
            "positions": list(range(15)),
        },
    }

    mock_tool.ainvoke = AsyncMock(return_value=json.dumps(reco_data))
    mock_get_mcp_tools.return_value = [mock_tool]

    await get_few_shot_examples("test query", tenant_id=1)

    _, kwargs = mock_update_trace.call_args
    metadata = kwargs["metadata"]

    assert metadata["recommendation.selected.truncated"] is True
    fingerprints = json.loads(metadata["recommendation.selected.fingerprints"])
    assert len(fingerprints) == 10
    assert fingerprints[0] == "F0"
    assert fingerprints[-1] == "F9"


@pytest.mark.asyncio
@patch("agent_core.nodes.generate.telemetry.start_span")
@patch("agent_core.nodes.generate.telemetry.update_current_trace")
@patch("agent_core.tools.get_mcp_tools")
async def test_recommendation_telemetry_bounding_chars(
    mock_get_mcp_tools, mock_update_trace, mock_start_span
):
    """Test that JSON string length is capped (at element level)."""
    mock_span = MagicMock()
    mock_start_span.return_value.__enter__.return_value = mock_span
    mock_tool = MagicMock()
    mock_tool.name = "recommend_examples"

    # Large fingerprints to exceed 4KB with small number of items
    # Each item ~1000 chars. 5 items = 5000 chars > 4096 limit.
    large_items = ["x" * 1000 for _ in range(5)]
    reco_data = {
        "examples": [],
        "metadata": {
            "fingerprints": large_items,
        },
    }

    mock_tool.ainvoke = AsyncMock(return_value=json.dumps(reco_data))
    mock_get_mcp_tools.return_value = [mock_tool]

    await get_few_shot_examples("test query", tenant_id=1)

    _, kwargs = mock_update_trace.call_args
    metadata = kwargs["metadata"]

    json_str = metadata["recommendation.selected.fingerprints"]
    assert len(json_str) <= 4096
    items = json.loads(json_str)
    assert len(items) < 5
    assert metadata["recommendation.selected.truncated"] is True


@pytest.mark.asyncio
@patch("agent_core.nodes.generate.telemetry.start_span")
@patch("agent_core.nodes.generate.telemetry.update_current_trace")
@patch("agent_core.tools.get_mcp_tools")
async def test_recommendation_telemetry_indexing(
    mock_get_mcp_tools, mock_update_trace, mock_start_span
):
    """Test that indexing fields are attached to spans."""
    mock_span = MagicMock()
    mock_start_span.return_value.__enter__.return_value = mock_span
    mock_tool = MagicMock()
    mock_tool.name = "recommend_examples"

    mock_tool.ainvoke = AsyncMock(return_value=json.dumps({"examples": []}))
    mock_get_mcp_tools.return_value = [mock_tool]

    await get_few_shot_examples("test query", tenant_id=99, interaction_id="int-123")

    # Check child span
    mock_span.set_attribute.assert_any_call("tenant_id", 99)
    mock_span.set_attribute.assert_any_call("interaction_id", "int-123")


@pytest.mark.asyncio
@patch("agent_core.nodes.generate.telemetry.start_span")
@patch("agent_core.nodes.generate.telemetry.update_current_trace")
@patch("agent_core.tools.get_mcp_tools")
async def test_recommendation_telemetry_fail_safe(
    mock_get_mcp_tools, mock_update_trace, mock_start_span
):
    """Test fail-safe behavior when metadata is broken."""
    mock_span = MagicMock()
    mock_start_span.return_value.__enter__.return_value = mock_span
    mock_tool = MagicMock()
    mock_tool.name = "recommend_examples"

    # Malformed metadata (e.g. string instead of dict) to trigger exception in helper
    reco_data = {
        "examples": [],
        "metadata": "THIS IS NOT A DICT",
    }

    mock_tool.ainvoke = AsyncMock(return_value=json.dumps(reco_data))
    mock_get_mcp_tools.return_value = [mock_tool]

    # Should not raise
    await get_few_shot_examples("test query", tenant_id=1)

    # Should have fallback to minimal telemetry
    mock_update_trace.assert_called()
    _, kwargs = mock_update_trace.call_args
    metadata = kwargs["metadata"]
    assert metadata["recommendation.used"] is True


def test_otel_worker_compatibility_scalars():
    """Assertion helper (not a test on its own but could be).

    Ensures all keys in a telemetry dict are scalar.
    """
    sample_metadata = {
        "recommendation.used": True,
        "recommendation.count.total": 1,
        "recommendation.selected.fingerprints": "[]",
    }
    for k, v in sample_metadata.items():
        assert isinstance(v, (str, int, bool, float))
