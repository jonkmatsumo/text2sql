"""Regression tests for execution invariants and guardrails."""

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from langchain_core.messages import HumanMessage

from agent.graph import route_after_execution
from agent.nodes.execute import validate_and_execute_node
from agent.state import AgentState
from agent.telemetry import InMemoryTelemetryBackend, telemetry
from common.models.tool_envelopes import ExecuteSQLQueryMetadata, ExecuteSQLQueryResponseEnvelope


@pytest.fixture
def telemetry_backend():
    """Reset and return the in-memory telemetry backend."""
    backend = InMemoryTelemetryBackend()
    telemetry.set_backend(backend)
    return backend


@pytest.mark.asyncio
async def test_invariant_reason_codes_on_decisions(telemetry_backend):
    """Ensure reason codes are always present on system decisions."""
    state = AgentState(
        messages=[HumanMessage(content="test")],
        current_sql="SELECT 1",
        retry_count=0,
        interactive_session=True,
        page_size=10,  # Required for prefetch candidate check
    )

    # Mock tool to return a next_page_token to trigger prefetch decision
    # Using envelope structure now
    mock_payload = {
        "schema_version": "1.0",
        "rows": [{"id": 1}],
        "metadata": {"rows_returned": 1, "next_page_token": "token-123", "is_truncated": False},
    }

    with (
        patch("agent.nodes.execute.get_mcp_tools") as mock_get_tools,
        patch("agent.nodes.execute.PolicyEnforcer.validate_sql", return_value=None),
        patch("agent.nodes.execute.TenantRewriter.rewrite_sql", side_effect=lambda sql, tid: sql),
    ):
        mock_tool = MagicMock()
        mock_tool.name = "execute_sql_query"
        mock_tool.ainvoke = AsyncMock(return_value=json.dumps(mock_payload))
        mock_get_tools.return_value = [mock_tool]

        await validate_and_execute_node(state)

        # Check telemetry for system.decision event
        found_prefetch = False
        for span in telemetry_backend.spans:
            for event in span.events:
                if event["name"] == "system.decision":
                    attrs = event["attributes"]
                    assert "reason_code" in attrs
                    if attrs["action"] == "prefetch":
                        found_prefetch = True

        assert found_prefetch


def test_invariant_no_raw_sql_in_telemetry_attributes(telemetry_backend):
    """Verify that raw SQL is never attached to spans, only hashes or summaries."""
    sql = "SELECT * FROM users WHERE secret_key = 'PII_123'"

    with telemetry.start_span("test_span") as span:
        span.set_attribute("failed_sql", sql)

    final_span = telemetry_backend.spans[0]
    val = final_span.attributes.get("failed_sql")
    assert val != sql
    assert "hash:" in str(val)


def test_invariant_retry_limit_enforced():
    """Ensure retries never exceed configured limits."""
    # AGENT_MAX_RETRIES defaults to 3
    state = AgentState(
        messages=[HumanMessage(content="test")],
        current_sql="SELECT 1",
        error='relation "missing" does not exist',
        error_category="syntax",
        retry_count=3,  # Already at limit
    )

    # route_after_execution should return "failed"
    decision = route_after_execution(state)
    assert decision == "failed"


@pytest.mark.asyncio
async def test_invariant_payload_bounding_in_telemetry(telemetry_backend):
    """Verify that large payloads are bounded in telemetry."""
    # Create a huge list of rows
    large_result = [{"id": i, "data": "x" * 100} for i in range(1000)]

    from agent.telemetry import OTELTelemetrySpan

    mock_otel_span = MagicMock()
    otel_span = OTELTelemetrySpan(mock_otel_span)

    otel_span.set_outputs({"query_result": large_result})

    # Check call to mock_otel_span.set_attribute
    found_output = False
    for call in mock_otel_span.set_attribute.call_args_list:
        if call[0][0] == "telemetry.outputs_json":
            found_output = True
            val = call[0][1]
            assert len(val) < 35000  # Close to 32KB
            assert "... [TRUNCATED]" in val
    assert found_output


@pytest.mark.asyncio
async def test_execute_completeness_contract(telemetry_backend):
    """Verify that execution result populates all completeness contract fields."""
    state = AgentState(
        messages=[HumanMessage(content="test")],
        current_sql="SELECT 1",
        retry_count=0,
    )

    # Mock envelope with explicit truncation
    envelope = ExecuteSQLQueryResponseEnvelope(
        rows=[{"id": 1}],
        metadata=ExecuteSQLQueryMetadata(
            rows_returned=100, is_truncated=True, is_limited=False, partial_reason="PROVIDER_CAP"
        ),
    )

    with (
        patch("agent.nodes.execute.get_mcp_tools") as mock_get_tools,
        patch("agent.nodes.execute.PolicyEnforcer.validate_sql", return_value=None),
        patch("agent.nodes.execute.TenantRewriter.rewrite_sql", side_effect=lambda sql, tid: sql),
    ):
        mock_tool = MagicMock()
        mock_tool.name = "execute_sql_query"
        # We can return the model dump as dict or json string, agent handles both via shim/parser
        mock_tool.ainvoke = AsyncMock(return_value=envelope.model_dump_json())
        mock_get_tools.return_value = [mock_tool]

        result = await validate_and_execute_node(state)

        assert "result_completeness" in result
        comp = result["result_completeness"]
        assert comp["is_truncated"] is True
        assert comp["partial_reason"] == "PROVIDER_CAP"
        assert comp["rows_returned"] == 100
