"""Unit tests for SQL execution and validation node."""

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from agent.nodes.execute import validate_and_execute_node
from agent.state import AgentState


def _envelope(rows=None, metadata=None, error=None):
    data = {
        "schema_version": "1.0",
        "rows": rows if rows is not None else [],
        "metadata": (
            metadata
            if metadata
            else {"rows_returned": len(rows) if rows else 0, "is_truncated": False}
        ),
    }
    if error:
        data["error"] = error
        if "rows_returned" not in data["metadata"]:
            data["metadata"]["rows_returned"] = 0
    return json.dumps(data)


class TestValidateAndExecuteNode:
    """Unit tests for validate_and_execute_node function."""

    @pytest.mark.asyncio
    @patch("agent.nodes.execute.get_mcp_tools")
    @patch("agent.nodes.execute.PolicyEnforcer")
    @patch("agent.nodes.execute.TenantRewriter")
    async def test_validate_and_execute_node_success_json_string(
        self, mock_rewriter, mock_enforcer, mock_get_tools, schema_fixture
    ):
        """Test successful query execution with JSON string result."""
        # Mock enforcer to pass
        mock_enforcer.validate_sql.return_value = None
        # Mock rewriter to return same SQL
        mock_rewriter.rewrite_sql = AsyncMock(side_effect=lambda sql, tid: sql)

        # Create mock tool
        mock_tool = AsyncMock()
        mock_tool.name = "execute_sql_query"
        mock_tool.ainvoke = AsyncMock(return_value=_envelope(rows=[{"count": 1000}]))

        mock_get_tools.return_value = [mock_tool]

        state = AgentState(
            messages=[],
            schema_context="",
            current_sql=schema_fixture.count_query,
            query_result=None,
            error=None,
            retry_count=0,
            tenant_id=None,  # Explicitly set to None for this test
        )

        await validate_and_execute_node(state)

        # Verify tool was called with correct query (tenant_id is extracted from context)
        mock_tool.ainvoke.assert_called_once()
        call_args = mock_tool.ainvoke.call_args[0][0]
        assert call_args["sql_query"] == schema_fixture.count_query

    @pytest.mark.asyncio
    @patch("agent.nodes.execute.get_mcp_tools")
    @patch("agent.nodes.execute.PolicyEnforcer")
    @patch("agent.nodes.execute.TenantRewriter")
    async def test_execute_sql_node_uses_replay_bundle(
        self, mock_rewriter, mock_enforcer, mock_get_tools, schema_fixture
    ):
        """Replay bundle should bypass live tool calls if matching SQL is found."""
        mock_enforcer.validate_sql.return_value = None
        mock_rewriter.rewrite_sql = AsyncMock(side_effect=lambda sql, tid: sql)

        mock_tool = AsyncMock()
        mock_tool.name = "execute_sql_query"
        mock_get_tools.return_value = [mock_tool]

        replay_bundle = {
            "tool_io": [
                {
                    "name": "execute_sql_query",
                    "input": {"sql_query": "SELECT count(*) FROM users"},
                    # The replay bundle might have legacy shape or new shape.
                    # If legacy, we assume shim is enabled or we update bundle.
                    # Let's update bundle mock to new shape for this test.
                    "output": json.loads(_envelope(rows=[{"count": 42}])),
                }
            ]
        }

        state = AgentState(
            messages=[],
            schema_context="",
            current_sql="SELECT count(*) FROM users",
            query_result=None,
            error=None,
            retry_count=0,
            replay_bundle=replay_bundle,
        )

        result = await validate_and_execute_node(state)

        # Verify tool was NOT called
        mock_tool.ainvoke.assert_not_called()
        assert result["query_result"] == [{"count": 42}]
        assert result["error"] is None

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    @patch("agent.nodes.execute.get_mcp_tools")
    @patch("agent.nodes.execute.PolicyEnforcer")
    @patch("agent.nodes.execute.TenantRewriter")
    async def test_validate_and_execute_node_success_dict_result(
        self, mock_rewriter, mock_enforcer, mock_get_tools, schema_fixture
    ):
        """Test successful query execution with dict result (parsed envelope)."""
        # Mock enforcer to pass
        mock_enforcer.validate_sql.return_value = None
        # Mock rewriter to return same SQL
        mock_rewriter.rewrite_sql = AsyncMock(side_effect=lambda sql, tid: sql)

        mock_tool = AsyncMock()
        mock_tool.name = "execute_sql_query"
        # Tool usually returns string, but if it returned dict, parser handles it.
        mock_tool.ainvoke = AsyncMock(
            return_value=json.loads(_envelope(rows=[{"id": 1, "title": "Film 1"}]))
        )

        mock_get_tools.return_value = [mock_tool]

        state = AgentState(
            messages=[],
            schema_context="",
            current_sql=schema_fixture.sample_query,
            query_result=None,
            error=None,
            retry_count=0,
        )

        result = await validate_and_execute_node(state)

        assert result["query_result"] == [{"id": 1, "title": "Film 1"}]
        assert result["error"] is None

    @pytest.mark.asyncio
    @patch("agent.nodes.execute.get_mcp_tools")
    @patch("agent.nodes.execute.PolicyEnforcer")
    @patch("agent.nodes.execute.TenantRewriter")
    async def test_execute_parses_truncation_envelope(
        self, mock_rewriter, mock_enforcer, mock_get_tools, schema_fixture
    ):
        """Test execution parses truncation metadata envelope."""
        mock_enforcer.validate_sql.return_value = None
        mock_rewriter.rewrite_sql = AsyncMock(side_effect=lambda sql, tid: sql)
        mock_tool = AsyncMock()
        mock_tool.name = "execute_sql_query"
        payload = _envelope(
            rows=[{"id": 1}], metadata={"is_truncated": True, "row_limit": 100, "rows_returned": 1}
        )
        mock_tool.ainvoke = AsyncMock(return_value=payload)

        mock_get_tools.return_value = [mock_tool]

        state = AgentState(
            messages=[],
            schema_context="",
            current_sql=schema_fixture.sample_query,
            query_result=None,
            error=None,
            retry_count=0,
        )

        result = await validate_and_execute_node(state)

        assert result["query_result"] == [{"id": 1}]
        assert result["error"] is None
        assert result["result_is_truncated"] is True
        assert result["result_row_limit"] == 100
        assert result["result_rows_returned"] == 1

    @pytest.mark.asyncio
    @patch("agent.nodes.execute.get_mcp_tools")
    @patch("agent.nodes.execute.PolicyEnforcer")
    @patch("agent.nodes.execute.TenantRewriter")
    async def test_execute_requests_include_columns(
        self, mock_rewriter, mock_enforcer, mock_get_tools, schema_fixture
    ):
        """Ensure execute tool requests column metadata."""
        mock_enforcer.validate_sql.return_value = None
        mock_rewriter.rewrite_sql = AsyncMock(side_effect=lambda sql, tid: sql)
        mock_tool = AsyncMock()
        mock_tool.name = "execute_sql_query"
        mock_tool.ainvoke = AsyncMock(return_value=_envelope(rows=[]))

        mock_get_tools.return_value = [mock_tool]

        state = AgentState(
            messages=[],
            schema_context="",
            current_sql=schema_fixture.sample_query,
            query_result=None,
            error=None,
            retry_count=0,
        )

        await validate_and_execute_node(state)

        call_args = mock_tool.ainvoke.call_args[0][0]
        assert call_args["include_columns"] is True

    @pytest.mark.asyncio
    @patch("agent.nodes.execute.get_mcp_tools")
    @patch("agent.nodes.execute.PolicyEnforcer")
    @patch("agent.nodes.execute.TenantRewriter")
    async def test_execute_stores_result_columns(
        self, mock_rewriter, mock_enforcer, mock_get_tools, schema_fixture
    ):
        """Store column metadata from tool envelope."""
        mock_enforcer.validate_sql.return_value = None
        mock_rewriter.rewrite_sql = AsyncMock(side_effect=lambda sql, tid: sql)
        mock_tool = AsyncMock()
        mock_tool.name = "execute_sql_query"
        columns = [{"name": "id", "type": "int"}]

        env = json.loads(_envelope(rows=[{"id": 1}]))
        env["columns"] = columns
        payload = json.dumps(env)

        mock_tool.ainvoke = AsyncMock(return_value=payload)

        mock_get_tools.return_value = [mock_tool]

        state = AgentState(
            messages=[],
            schema_context="",
            current_sql=schema_fixture.sample_query,
            query_result=None,
            error=None,
            retry_count=0,
        )

        result = await validate_and_execute_node(state)

        assert result["result_columns"] == columns

    @pytest.mark.asyncio
    @patch("agent.nodes.execute.get_mcp_tools")
    @patch("agent.nodes.execute.PolicyEnforcer")
    @patch("agent.nodes.execute.TenantRewriter")
    async def test_execute_computes_remaining_timeout(
        self, mock_rewriter, mock_enforcer, mock_get_tools, schema_fixture
    ):
        """Pass remaining time budget to execute tool."""
        mock_enforcer.validate_sql.return_value = None
        mock_rewriter.rewrite_sql = AsyncMock(side_effect=lambda sql, tid: sql)
        mock_tool = AsyncMock()
        mock_tool.name = "execute_sql_query"
        mock_tool.ainvoke = AsyncMock(return_value=_envelope(rows=[]))

        mock_get_tools.return_value = [mock_tool]

        import time

        deadline_ts = time.monotonic() + 1.0
        state = AgentState(
            messages=[],
            schema_context="",
            current_sql=schema_fixture.sample_query,
            query_result=None,
            error=None,
            retry_count=0,
            deadline_ts=deadline_ts,
        )

        await validate_and_execute_node(state)

        call_args = mock_tool.ainvoke.call_args[0][0]
        assert 0 < call_args["timeout_seconds"] <= 1.0

    @pytest.mark.asyncio
    @patch("agent.nodes.execute.get_mcp_tools")
    @patch("agent.nodes.execute.PolicyEnforcer")
    @patch("agent.nodes.execute.TenantRewriter")
    async def test_execute_fails_fast_when_no_budget(
        self, mock_rewriter, mock_enforcer, mock_get_tools, schema_fixture
    ):
        """Fail fast when there is no remaining budget."""
        mock_enforcer.validate_sql.return_value = None
        mock_rewriter.rewrite_sql = AsyncMock(side_effect=lambda sql, tid: sql)
        mock_tool = AsyncMock()
        mock_tool.name = "execute_sql_query"
        mock_tool.ainvoke = AsyncMock()

        mock_get_tools.return_value = [mock_tool]

        import time

        state = AgentState(
            messages=[],
            schema_context="",
            current_sql=schema_fixture.sample_query,
            query_result=None,
            error=None,
            retry_count=0,
            deadline_ts=time.monotonic() - 1.0,
        )

        result = await validate_and_execute_node(state)

        assert result["error"] == "Execution timed out before query could start."
        assert result["error_category"] == "timeout"
        assert mock_tool.ainvoke.call_count == 0

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    @patch("agent.nodes.execute.get_mcp_tools")
    @patch("agent.nodes.execute.PolicyEnforcer")
    @patch("agent.nodes.execute.TenantRewriter")
    async def test_validate_and_execute_node_error_string(
        self, mock_rewriter, mock_enforcer, mock_get_tools, schema_fixture
    ):
        """Test error handling when tool returns error string."""
        # Mock enforcer to pass
        mock_enforcer.validate_sql.return_value = None
        # Mock rewriter to return same SQL
        mock_rewriter.rewrite_sql = AsyncMock(side_effect=lambda sql, tid: sql)
        mock_tool = AsyncMock()
        mock_tool.name = "execute_sql_query"
        error_msg = f"Error: relation '{schema_fixture.invalid_table}' does not exist"
        # Return error envelope
        mock_tool.ainvoke = AsyncMock(
            return_value=_envelope(
                error={
                    "message": error_msg,
                    "category": "unknown",
                    "provider": "test",
                    "is_retryable": False,
                }
            )
        )

        mock_get_tools.return_value = [mock_tool]

        state = AgentState(
            messages=[],
            schema_context="",
            current_sql=f"SELECT * FROM {schema_fixture.invalid_table}",
            query_result=None,
            error=None,
            retry_count=0,
        )

        result = await validate_and_execute_node(state)

        assert result["error"] == error_msg
        assert result["query_result"] is None

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    @patch("agent.nodes.execute.get_mcp_tools")
    @patch("agent.nodes.execute.PolicyEnforcer")
    @patch("agent.nodes.execute.TenantRewriter")
    async def test_validate_and_execute_node_database_error(
        self, mock_rewriter, mock_enforcer, mock_get_tools
    ):
        """Test error handling when tool returns database error string."""
        # Mock enforcer to pass
        mock_enforcer.validate_sql.return_value = None
        # Mock rewriter to return same SQL
        mock_rewriter.rewrite_sql = AsyncMock(side_effect=lambda sql, tid: sql)
        mock_tool = AsyncMock()
        mock_tool.name = "execute_sql_query"
        error_msg = "Database Error: syntax error at or near 'FROM'"
        mock_tool.ainvoke = AsyncMock(
            return_value=_envelope(
                error={
                    "message": error_msg,
                    "category": "syntax",
                    "provider": "test",
                    "is_retryable": False,
                }
            )
        )

        mock_get_tools.return_value = [mock_tool]

        state = AgentState(
            messages=[],
            schema_context="",
            current_sql="SELECT FROM",
            query_result=None,
            error=None,
            retry_count=0,
        )

        result = await validate_and_execute_node(state)

        assert "Database Error" in result["error"]
        assert result["query_result"] is None

    @pytest.mark.asyncio
    @patch("agent.nodes.execute.get_mcp_tools")
    @patch("agent.nodes.execute.PolicyEnforcer")
    @patch("agent.nodes.execute.TenantRewriter")
    async def test_validate_and_execute_node_error_category(
        self, mock_rewriter, mock_enforcer, mock_get_tools
    ):
        """Test error handling when tool returns categorized error payload."""
        mock_enforcer.validate_sql.return_value = None
        mock_rewriter.rewrite_sql = AsyncMock(side_effect=lambda sql, tid: sql)
        mock_tool = AsyncMock()
        mock_tool.name = "execute_sql_query"

        mock_tool.ainvoke = AsyncMock(
            return_value=_envelope(
                error={
                    "message": "Database Error: syntax error",
                    "category": "syntax",
                    "provider": "test",
                    "is_retryable": False,
                }
            )
        )

        mock_get_tools.return_value = [mock_tool]

        state = AgentState(
            messages=[],
            schema_context="",
            current_sql="SELECT FROM",
            query_result=None,
            error=None,
            retry_count=0,
        )

        result = await validate_and_execute_node(state)

        assert result["error"] == "Database Error: syntax error"
        assert result["error_category"] == "syntax"
        assert result["query_result"] is None

    @pytest.mark.asyncio
    @patch("agent.nodes.execute.get_mcp_tools")
    @patch("agent.nodes.execute.PolicyEnforcer")
    @patch("agent.nodes.execute.TenantRewriter")
    async def test_validate_and_execute_node_missing_tool(
        self, mock_rewriter, mock_enforcer, mock_get_tools, schema_fixture
    ):
        """Test error handling when execute_sql_query is not found."""
        # Mock enforcer to pass
        mock_enforcer.validate_sql.return_value = None
        # Mock rewriter to return same SQL
        mock_rewriter.rewrite_sql = AsyncMock(side_effect=lambda sql, tid: sql)

        # Return tools without execute_sql_query
        mock_tool = MagicMock()
        mock_tool.name = "list_tables"
        mock_get_tools.return_value = [mock_tool]

        state = AgentState(
            messages=[],
            schema_context="",
            current_sql=schema_fixture.sample_query,
            query_result=None,
            error=None,
            retry_count=0,
        )

        result = await validate_and_execute_node(state)

        assert result["error"] == "execute_sql_query tool not found in MCP server"
        assert result["query_result"] is None

    @pytest.mark.asyncio
    @patch("agent.nodes.execute.get_mcp_tools")
    async def test_validate_and_execute_node_no_sql(self, mock_get_tools):
        """Test error handling when current_sql is None."""
        mock_tool = AsyncMock()
        mock_tool.name = "execute_sql_query"
        mock_get_tools.return_value = [mock_tool]

        state = AgentState(
            messages=[],
            schema_context="",
            current_sql=None,
            query_result=None,
            error=None,
            retry_count=0,
        )

        result = await validate_and_execute_node(state)

        assert result["error"] == "No SQL query to execute"
        assert result["query_result"] is None

    @pytest.mark.asyncio
    @patch("agent.nodes.execute.get_mcp_tools")
    async def test_validate_and_execute_node_empty_sql(self, mock_get_tools):
        """Test error handling when current_sql is empty string."""
        mock_tool = AsyncMock()
        mock_tool.name = "execute_sql_query"
        mock_get_tools.return_value = [mock_tool]

        state = AgentState(
            messages=[],
            schema_context="",
            current_sql="",
            query_result=None,
            error=None,
            retry_count=0,
        )

        result = await validate_and_execute_node(state)

        assert result["error"] == "No SQL query to execute"
        assert result["query_result"] is None

    @pytest.mark.asyncio
    @patch("agent.nodes.execute.get_mcp_tools")
    @patch("agent.nodes.execute.PolicyEnforcer")
    @patch("agent.nodes.execute.TenantRewriter")
    async def test_validate_and_execute_node_execution_exception(
        self, mock_rewriter, mock_enforcer, mock_get_tools, schema_fixture
    ):
        """Test error handling when tool execution raises exception."""
        # Mock enforcer to pass
        mock_enforcer.validate_sql.return_value = None
        # Mock rewriter to return same SQL
        mock_rewriter.rewrite_sql = AsyncMock(side_effect=lambda sql, tid: sql)

        mock_tool = AsyncMock()
        mock_tool.name = "execute_sql_query"
        mock_tool.ainvoke = AsyncMock(side_effect=Exception("Connection timeout"))

        mock_get_tools.return_value = [mock_tool]

        state = AgentState(
            messages=[],
            schema_context="",
            current_sql=schema_fixture.sample_query,
            query_result=None,
            error=None,
            retry_count=0,
        )

        result = await validate_and_execute_node(state)

        # Exception from tool call inside TaskGroup will be wrapped
        assert "Connection timeout" in result["error"]
        assert result["query_result"] is None

    @pytest.mark.asyncio
    @patch("agent.nodes.execute.get_mcp_tools")
    @patch("agent.nodes.execute.PolicyEnforcer")
    @patch("agent.nodes.execute.TenantRewriter")
    async def test_validate_and_execute_node_invalid_json(
        self, mock_rewriter, mock_enforcer, mock_get_tools, schema_fixture
    ):
        """Test handling when result is string but not valid JSON (Malformed)."""
        # Mock enforcer to pass
        mock_enforcer.validate_sql.return_value = None
        # Mock rewriter to return same SQL
        mock_rewriter.rewrite_sql = AsyncMock(side_effect=lambda sql, tid: sql)

        mock_tool = AsyncMock()
        mock_tool.name = "execute_sql_query"
        mock_tool.ainvoke = AsyncMock(return_value="Error: Invalid JSON string")

        mock_get_tools.return_value = [mock_tool]

        state = AgentState(
            messages=[],
            schema_context="",
            current_sql=schema_fixture.sample_query,
            query_result=None,
            error=None,
            retry_count=0,
        )

        result = await validate_and_execute_node(state)

        # Should treat invalid JSON as error envelope with Malformed message
        # (via _create_error_envelope fallback).
        # _parse_tool_response_with_shim -> parse_execute_sql_response ->
        # _create_error_envelope(payload)
        # So error message is the payload string itself? No.
        # `parse_execute_sql_response`:
        # if isinstance(payload, str): try json.loads...
        # except: return _create_error_envelope(payload)
        # So yes, error message is "Error: Invalid JSON string".

        assert result["error"] == "Error: Invalid JSON string"
        assert result["query_result"] is None

    @pytest.mark.asyncio
    @patch("agent.nodes.execute.get_mcp_tools")
    @patch("agent.nodes.execute.PolicyEnforcer")
    @patch("agent.nodes.execute.TenantRewriter")
    async def test_validate_and_execute_node_empty_tools(
        self, mock_rewriter, mock_enforcer, mock_get_tools, schema_fixture
    ):
        """Test error handling when no tools are returned."""
        # Mock enforcer to pass
        mock_enforcer.validate_sql.return_value = None
        # Mock rewriter to return same SQL
        mock_rewriter.rewrite_sql = AsyncMock(side_effect=lambda sql, tid: sql)

        mock_get_tools.return_value = []

        state = AgentState(
            messages=[],
            schema_context="",
            current_sql=schema_fixture.sample_query,
            query_result=None,
            error=None,
            retry_count=0,
        )

        result = await validate_and_execute_node(state)

        assert result["error"] == "execute_sql_query tool not found in MCP server"
        assert result["query_result"] is None

    @pytest.mark.asyncio
    @patch("agent.nodes.execute.PolicyEnforcer")
    async def test_validate_and_execute_node_blocks_unsafe_sql(self, mock_enforcer, schema_fixture):
        """Test that PolicyEnforcer blocks unsafe SQL before tool invocation."""
        error_msg = f"Access to table '{schema_fixture.valid_table}' is not allowed"
        mock_enforcer.validate_sql.side_effect = ValueError(error_msg)

        state = AgentState(
            messages=[],
            schema_context="",
            current_sql=f"SELECT * FROM {schema_fixture.valid_table}",
            query_result=None,
            error=None,
            retry_count=0,
        )

        result = await validate_and_execute_node(state)

        assert "Security Policy Violation" in result["error"]
        assert error_msg in result["error"]
        assert result["query_result"] is None

    @pytest.mark.asyncio
    @patch("agent.nodes.execute.telemetry.start_span")
    @patch("agent.nodes.execute.get_mcp_tools")
    @patch("agent.nodes.execute.PolicyEnforcer")
    @patch("agent.nodes.execute.TenantRewriter")
    async def test_execute_sql_node_fails_on_incompatible_version(
        self, mock_rewriter, mock_enforcer, mock_get_tools, mock_start_span, schema_fixture
    ):
        """Node should fail if the tool returns an incompatible major version."""
        mock_enforcer.validate_sql.return_value = None
        mock_rewriter.rewrite_sql = AsyncMock(side_effect=lambda sql, tid: sql)

        mock_tool = AsyncMock()
        mock_tool.name = "execute_sql_query"
        # Return a version 2.0 payload
        data = {
            "schema_version": "2.0",
            "rows": [{"id": 1}],
            "metadata": {"rows_returned": 1, "is_truncated": False},
        }
        mock_tool.ainvoke = AsyncMock(return_value=json.dumps(data))

        mock_get_tools.return_value = [mock_tool]

        state = AgentState(
            messages=[],
            schema_context="",
            current_sql="SELECT 1",
            query_result=None,
            error=None,
            retry_count=0,
        )

        result = await validate_and_execute_node(state)

        assert result["error"] is not None
        assert "Incompatible envelope version" in result["error"]
        assert "2.0" in result["error"]

    @pytest.mark.asyncio
    @patch("agent.nodes.execute.telemetry.start_span")
    @patch("agent.nodes.execute.get_mcp_tools")
    @patch("agent.nodes.execute.PolicyEnforcer")
    @patch("agent.nodes.execute.TenantRewriter")
    async def test_execute_sql_node_accepts_stable_minor_version(
        self, mock_rewriter, mock_enforcer, mock_get_tools, mock_start_span, schema_fixture
    ):
        """Node should accept a minor version update (e.g., 1.1) from the tool."""
        mock_enforcer.validate_sql.return_value = None
        mock_rewriter.rewrite_sql = AsyncMock(side_effect=lambda sql, tid: sql)

        mock_tool = AsyncMock()
        mock_tool.name = "execute_sql_query"
        # Return a version 1.1 payload
        data = {
            "schema_version": "1.1",
            "rows": [{"id": 1}],
            "metadata": {"rows_returned": 1, "is_truncated": False},
        }
        mock_tool.ainvoke = AsyncMock(return_value=json.dumps(data))
        mock_get_tools.return_value = [mock_tool]

        state = AgentState(
            messages=[],
            schema_context="",
            current_sql="SELECT 1",
            query_result=None,
            error=None,
            retry_count=0,
        )

        result = await validate_and_execute_node(state)
        assert result["error"] is None
        assert result["query_result"] == [{"id": 1}]
