"""Failure-mode matrix tests for routing and stopping-reason behavior."""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, patch

import pytest
from langchain_core.messages import HumanMessage

from agent.graph import route_after_execution
from agent.models.termination import TerminationReason
from agent.nodes.execute import validate_and_execute_node
from agent.state.decision_summary import build_retry_correction_summary


class _DummySpan:
    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return None

    def set_attribute(self, *_args, **_kwargs):
        return None

    def set_attributes(self, *_args, **_kwargs):
        return None

    def set_inputs(self, *_args, **_kwargs):
        return None

    def set_outputs(self, *_args, **_kwargs):
        return None

    def add_event(self, *_args, **_kwargs):
        return None


async def _run_execute_with_payload(state: dict, payload: dict) -> tuple[dict, AsyncMock]:
    execute_tool = AsyncMock()
    execute_tool.name = "execute_sql_query"
    execute_tool.ainvoke = AsyncMock(return_value=json.dumps(payload))

    with (
        patch("agent.nodes.execute.get_mcp_tools", AsyncMock(return_value=[execute_tool])),
        patch("agent.nodes.execute.PolicyEnforcer") as mock_enforcer,
        patch("agent.nodes.execute.TenantRewriter") as mock_rewriter,
        patch("agent.nodes.execute.telemetry.start_span", return_value=_DummySpan()),
    ):
        mock_enforcer.validate_sql.return_value = None
        mock_rewriter.rewrite_sql = AsyncMock(side_effect=lambda sql, tid: sql)
        result = await validate_and_execute_node(state)

    return result, execute_tool


@pytest.mark.asyncio
async def test_failure_mode_matrix_covers_stopping_reason_and_output_shape(monkeypatch):
    """Matrix coverage should preserve expected routing and output contracts for key failures."""
    monkeypatch.setenv("MCP_USER_ROLE", "SQL_ADMIN_ROLE,TABLE_ADMIN_ROLE,SQL_USER_ROLE")
    monkeypatch.setenv("AGENT_AUTO_PAGINATION", "off")
    monkeypatch.setenv("AGENT_SCHEMA_BINDING_VALIDATION", "false")

    base_state = {
        "messages": [HumanMessage(content="Show orders")],
        "schema_context": "",
        "current_sql": "SELECT id FROM orders",
        "query_result": None,
        "tenant_id": 1,
        "retry_count": 0,
    }

    timeout_payload = {
        "schema_version": "1.0",
        "rows": [],
        "metadata": {"rows_returned": 0, "is_truncated": False, "tool_version": "v1"},
        "error": {
            "message": "Execution timed out.",
            "category": "timeout",
            "provider": "postgres",
            "is_retryable": True,
        },
    }
    timeout_result, _ = await _run_execute_with_payload(base_state.copy(), timeout_payload)
    timeout_state = {**base_state, **timeout_result}
    timeout_route = route_after_execution(timeout_state)
    timeout_summary = build_retry_correction_summary(timeout_state)
    assert timeout_route == "correct"
    assert timeout_summary["final_stopping_reason"] in {"timeout", "TerminationReason.TIMEOUT"}
    assert {"error", "error_category", "error_metadata"} <= set(timeout_result.keys())

    tenant_mismatch_payload = {
        "schema_version": "1.0",
        "rows": [],
        "metadata": {"rows_returned": 0, "is_truncated": False, "tool_version": "v1"},
        "error": {
            "message": "Tenant mismatch",
            "category": "invalid_request",
            "provider": "postgres",
            "is_retryable": False,
            "code": "TENANT_SCOPE_VIOLATION",
        },
    }
    tenant_result, _ = await _run_execute_with_payload(base_state.copy(), tenant_mismatch_payload)
    tenant_state = {**base_state, **tenant_result}
    tenant_route = route_after_execution(tenant_state)
    tenant_summary = build_retry_correction_summary(tenant_state)
    assert tenant_route == "failed"
    assert str(tenant_state["retry_reason"]).lower() == "non_retryable_category"
    assert str(tenant_summary["final_stopping_reason"]).lower() == "non_retryable_category"
    assert tenant_result["error_metadata"]["code"] == "TENANT_SCOPE_VIOLATION"

    truncation_payload = {
        "schema_version": "1.0",
        "rows": [{"id": 1}],
        "metadata": {
            "rows_returned": 1,
            "is_truncated": True,
            "partial_reason": "MAX_ROWS",
            "tool_version": "v1",
        },
    }
    truncation_result, _ = await _run_execute_with_payload(base_state.copy(), truncation_payload)
    truncation_state = {**base_state, **truncation_result}
    truncation_route = route_after_execution(truncation_state)
    truncation_summary = build_retry_correction_summary(truncation_state)
    assert truncation_route == "visualize"
    assert truncation_summary["final_stopping_reason"] in {"success", "TerminationReason.SUCCESS"}
    assert truncation_result["result_is_truncated"] is True
    assert truncation_result["result_completeness"]["partial_reason"] == "TRUNCATED"
    assert {"query_result", "result_completeness", "result_rows_returned"} <= set(
        truncation_result.keys()
    )

    monkeypatch.setenv("AGENT_BLOCK_ON_SCHEMA_MISMATCH", "true")
    mismatch_state = {
        **base_state,
        "current_sql": "SELECT missing_table.id FROM missing_table",
        "raw_schema_context": [
            {"type": "Table", "name": "orders"},
            {"type": "Column", "table": "orders", "name": "id"},
        ],
    }
    mismatch_result, mismatch_tool = await _run_execute_with_payload(
        mismatch_state.copy(),
        truncation_payload,
    )
    mismatch_summary = build_retry_correction_summary({**mismatch_state, **mismatch_result})
    assert mismatch_result["error_category"] == "schema_mismatch"
    assert mismatch_result["termination_reason"] == TerminationReason.VALIDATION_FAILED
    assert mismatch_summary["final_stopping_reason"] in {
        "schema_mismatch",
        "TerminationReason.VALIDATION_FAILED",
    }
    assert {"error", "error_category", "missing_identifiers"} <= set(mismatch_result.keys())
    mismatch_tool.ainvoke.assert_not_called()
