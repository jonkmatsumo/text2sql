"""Tests for run-scoped schema snapshot pinning and refresh propagation."""

import asyncio
import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from agent.nodes.retrieve import retrieve_context_node
from agent.state import AgentState
from agent.utils.schema_snapshot import apply_pending_schema_snapshot_refresh


def _mock_span_ctx(mock_start_span):
    mock_span = MagicMock()
    mock_start_span.return_value.__enter__ = MagicMock(return_value=mock_span)
    mock_start_span.return_value.__exit__ = MagicMock(return_value=False)
    return mock_span


@pytest.mark.asyncio
@patch("agent.nodes.retrieve.telemetry.start_span")
@patch("agent.nodes.retrieve.get_mcp_tools")
@patch("agent.utils.schema_fingerprint.resolve_schema_snapshot_id")
@patch("agent.utils.schema_fingerprint.fingerprint_schema_nodes")
async def test_concurrent_retrieve_runs_keep_pinned_snapshots_isolated(
    mock_fingerprint,
    mock_resolve_snapshot_id,
    mock_get_mcp_tools,
    mock_start_span,
):
    """Concurrent runs should not cross-contaminate pinned schema snapshots."""
    _mock_span_ctx(mock_start_span)
    mock_fingerprint.side_effect = lambda nodes: f"fp-{nodes[0]['name']}"

    def _resolve(nodes):
        names = [
            str(n.get("name")) for n in nodes if isinstance(n, dict) and n.get("type") == "Table"
        ]
        if "orders_a" in names:
            return "snap-a"
        if "orders_b" in names:
            return "snap-b"
        return "unknown"

    mock_resolve_snapshot_id.side_effect = _resolve

    calls: list[dict] = []

    async def _ainvoke(payload):
        calls.append(dict(payload))
        query = payload.get("query")
        if query == "run-a":
            return json.dumps(
                {"nodes": [{"type": "Table", "name": "orders_a"}], "relationships": []}
            )
        return json.dumps({"nodes": [{"type": "Table", "name": "orders_b"}], "relationships": []})

    mock_subgraph_tool = MagicMock()
    mock_subgraph_tool.name = "get_semantic_subgraph"
    mock_subgraph_tool.ainvoke = AsyncMock(side_effect=_ainvoke)
    mock_get_mcp_tools.return_value = [mock_subgraph_tool]

    from langchain_core.messages import HumanMessage

    state_a = AgentState(
        messages=[HumanMessage(content="run-a")],
        schema_context="",
        current_sql=None,
        query_result=None,
        error=None,
        retry_count=0,
        schema_snapshot_id="snap-a",
        pinned_schema_snapshot_id="snap-a",
    )
    state_b = AgentState(
        messages=[HumanMessage(content="run-b")],
        schema_context="",
        current_sql=None,
        query_result=None,
        error=None,
        retry_count=0,
        schema_snapshot_id="snap-b",
        pinned_schema_snapshot_id="snap-b",
    )

    result_a, result_b = await asyncio.gather(
        retrieve_context_node(state_a),
        retrieve_context_node(state_b),
    )

    assert result_a["schema_snapshot_id"] == "snap-a"
    assert result_a["pinned_schema_snapshot_id"] == "snap-a"
    assert result_b["schema_snapshot_id"] == "snap-b"
    assert result_b["pinned_schema_snapshot_id"] == "snap-b"

    # Confirm each retrieve call used its own pinned snapshot in tool payloads.
    payload_by_query = {payload["query"]: payload for payload in calls}
    assert payload_by_query["run-a"]["snapshot_id"] == "snap-a"
    assert payload_by_query["run-b"]["snapshot_id"] == "snap-b"


def test_schema_refresh_transition_applies_pending_snapshot_once():
    """Explicit refresh transition should update pinned snapshot exactly once."""
    initial_state = {
        "schema_snapshot_id": "snap-old",
        "pinned_schema_snapshot_id": "snap-old",
        "pending_schema_snapshot_id": "snap-new",
        "pending_schema_fingerprint": "fp-new",
        "pending_schema_version_ts": 123,
        "schema_snapshot_refresh_applied": 0,
    }

    first_refresh = apply_pending_schema_snapshot_refresh(
        initial_state,
        candidate_snapshot_id=initial_state["pending_schema_snapshot_id"],
        candidate_fingerprint=initial_state["pending_schema_fingerprint"],
        candidate_version_ts=initial_state["pending_schema_version_ts"],
    )
    assert first_refresh["schema_snapshot_id"] == "snap-new"
    assert first_refresh["pinned_schema_snapshot_id"] == "snap-new"
    assert first_refresh["pending_schema_snapshot_id"] is None
    assert first_refresh["schema_snapshot_refresh_applied"] == 1
    assert first_refresh["schema_snapshot_transition"]["old_snapshot_id"] == "snap-old"
    assert first_refresh["schema_snapshot_transition"]["new_snapshot_id"] == "snap-new"

    second_state = dict(initial_state)
    second_state.update(first_refresh)
    second_refresh = apply_pending_schema_snapshot_refresh(
        second_state,
        candidate_snapshot_id=None,
    )
    assert second_refresh["schema_snapshot_id"] == "snap-new"
    assert second_refresh["pinned_schema_snapshot_id"] == "snap-new"
    assert second_refresh["schema_snapshot_refresh_applied"] == 1
