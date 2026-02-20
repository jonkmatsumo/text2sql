"""Mocked end-to-end agent workflow coverage via `app.ainvoke()`."""

from __future__ import annotations

from typing import Any

import pytest

from agent.graph import MAX_CLARIFY_ROUNDS, app
from common.errors.error_codes import ErrorCode
from common.models.error_metadata import ErrorCategory
from mcp_server.utils.errors import build_error_metadata
from tests.utils.mock_agent_runtime import (
    MockDAL,
    MockMCPClient,
    build_app_input,
    install_mock_agent_runtime,
    unique_thread_config,
)


def _error_envelope(
    *,
    category: str,
    message: str,
    error_code: str,
    code: str = "TOOL_ERROR",
    retryable: bool = False,
) -> dict[str, Any]:
    try:
        normalized_category = ErrorCategory(category)
    except Exception:
        normalized_category = ErrorCategory.UNKNOWN

    error_payload = build_error_metadata(
        message=message,
        category=normalized_category,
        provider="mock",
        code=code,
        error_code=error_code,
        retryable=retryable,
    ).model_dump(exclude_none=True)

    return {
        "schema_version": "1.0",
        "rows": [],
        "metadata": {"rows_returned": 0, "is_truncated": False, "provider": "mock"},
        "error": error_payload,
    }


def _success_envelope(rows: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "schema_version": "1.0",
        "rows": rows,
        "metadata": {"rows_returned": len(rows), "is_truncated": False},
    }


def _value(value: Any) -> Any:
    return value.value if hasattr(value, "value") else value


@pytest.mark.asyncio
async def test_full_pipeline_successful_select_flow(monkeypatch):
    """Happy-path run should execute query and synthesize final answer."""
    dal = MockDAL(response=_success_envelope([{"value": 1}]))
    mcp = MockMCPClient(dal=dal)
    install_mock_agent_runtime(monkeypatch, mcp_client=mcp)

    state = build_app_input(
        question="Show one sample row",
        from_cache=True,
        current_sql="SELECT 1 AS value",
    )
    result = await app.ainvoke(state, config=unique_thread_config())

    assert result["error"] is None
    assert result["query_result"] == [{"value": 1}]
    assert result["messages"][-1].content == "Mocked synthesized response."
    assert len(dal.calls) == 1


@pytest.mark.asyncio
async def test_full_pipeline_policy_violation_flow(monkeypatch):
    """Mutating SQL should be rejected by validation before execute tool call."""
    dal = MockDAL(response=_success_envelope([{"ignored": True}]))
    mcp = MockMCPClient(dal=dal)
    install_mock_agent_runtime(monkeypatch, mcp_client=mcp)

    state = build_app_input(
        question="Drop customer table",
        from_cache=True,
        current_sql="DROP TABLE customer",
        retry_count=99,
    )
    result = await app.ainvoke(state, config=unique_thread_config())

    assert result["query_result"] is None
    assert "forbidden" in str(result["error"]).lower()
    assert len(dal.calls) == 0


@pytest.mark.asyncio
async def test_full_pipeline_tenant_enforcement_rejection_flow(monkeypatch):
    """Tenant-enforcement rejection should surface canonical tenant error code."""
    dal = MockDAL(
        response=_error_envelope(
            category="TENANT_ENFORCEMENT_UNSUPPORTED",
            message="Tenant isolation not supported for provider sqlite",
            error_code=ErrorCode.TENANT_ENFORCEMENT_UNSUPPORTED.value,
            code="TENANT_ENFORCEMENT_UNSUPPORTED",
        )
    )
    mcp = MockMCPClient(dal=dal)
    install_mock_agent_runtime(monkeypatch, mcp_client=mcp)

    state = build_app_input(
        question="Show orders",
        from_cache=True,
        current_sql="SELECT * FROM orders",
        retry_count=99,
    )
    result = await app.ainvoke(state, config=unique_thread_config())

    assert _value(result["error_category"]) == "TENANT_ENFORCEMENT_UNSUPPORTED"
    assert result["error_metadata"]["error_code"] == ErrorCode.TENANT_ENFORCEMENT_UNSUPPORTED.value
    assert "sqlite" not in str(result.get("error") or "").lower()
    assert (
        "tenant isolation is not supported for this provider"
        in str(result.get("error") or "").lower()
    )


@pytest.mark.asyncio
async def test_full_pipeline_ambiguity_unresolved_flow(monkeypatch):
    """Ambiguous requests should surface clarification state without DAL execution."""
    dal = MockDAL(response=_success_envelope([]))
    mcp = MockMCPClient(dal=dal)
    mcp.set_tool_response("lookup_cache", {"value": None})
    mcp.set_tool_response(
        "resolve_ambiguity",
        {
            "status": "AMBIGUOUS",
            "ambiguity_type": "schema_reference",
            "options": ["customer region", "store region"],
            "resolved_bindings": {},
        },
    )
    install_mock_agent_runtime(monkeypatch, mcp_client=mcp)

    state = build_app_input(
        question="Show revenue by region",
        clarify_count=MAX_CLARIFY_ROUNDS,
    )
    result = await app.ainvoke(state, config=unique_thread_config())

    assert _value(result["ambiguity_type"]) == "schema_reference"
    assert isinstance(result.get("clarification_question"), str)
    assert result["query_result"] is None
    assert len(dal.calls) == 0


@pytest.mark.asyncio
async def test_full_pipeline_db_timeout_propagation_flow(monkeypatch):
    """DB timeout errors should preserve canonical timeout error code."""
    dal = MockDAL(
        response=_error_envelope(
            category="timeout",
            message="Execution timed out.",
            error_code=ErrorCode.DB_TIMEOUT.value,
            code="DRIVER_TIMEOUT",
            retryable=True,
        )
    )
    mcp = MockMCPClient(dal=dal)
    install_mock_agent_runtime(monkeypatch, mcp_client=mcp)

    state = build_app_input(
        question="Run expensive query",
        from_cache=True,
        current_sql="SELECT * FROM huge_table",
        retry_count=99,
    )
    result = await app.ainvoke(state, config=unique_thread_config())

    assert _value(result["error_category"]) == "timeout"
    assert result["error_metadata"]["error_code"] == ErrorCode.DB_TIMEOUT.value
    assert "huge_table" not in str(result.get("error") or "").lower()
