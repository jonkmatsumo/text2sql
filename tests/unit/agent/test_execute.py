"""Unit tests for SQL execution and validation node."""

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from agent.nodes.execute import validate_and_execute_node
from agent.state import AgentState


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
        mock_tool.ainvoke = AsyncMock(return_value=json.dumps([{"count": 1000}]))

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

        result = await validate_and_execute_node(state)

        # Verify tool was called with correct query (tenant_id is extracted from context)
        mock_tool.ainvoke.assert_called_once()
        call_args = mock_tool.ainvoke.call_args[0][0]
        assert call_args["sql_query"] == schema_fixture.count_query
        assert call_args["tenant_id"] is None  # Mocks state tenant_id which is None here

        # Verify result was parsed and returned
        assert result["query_result"] == [{"count": 1000}]
        assert result["error"] is None

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    @patch("agent.nodes.execute.get_mcp_tools")
    @patch("agent.nodes.execute.PolicyEnforcer")
    @patch("agent.nodes.execute.TenantRewriter")
    async def test_validate_and_execute_node_success_dict_result(
        self, mock_rewriter, mock_enforcer, mock_get_tools, schema_fixture
    ):
        """Test successful query execution with dict result."""
        # Mock enforcer to pass
        mock_enforcer.validate_sql.return_value = None
        # Mock rewriter to return same SQL
        mock_rewriter.rewrite_sql = AsyncMock(side_effect=lambda sql, tid: sql)

        mock_tool = AsyncMock()
        mock_tool.name = "execute_sql_query"
        mock_tool.ainvoke = AsyncMock(return_value=[{"id": 1, "title": "Film 1"}])

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
    @pytest.mark.asyncio
    @patch("agent.nodes.execute.get_mcp_tools")
    @patch("agent.nodes.execute.PolicyEnforcer")
    @patch("agent.nodes.execute.TenantRewriter")
    async def test_validate_and_execute_node_success_list_result(
        self, mock_rewriter, mock_enforcer, mock_get_tools
    ):
        """Test successful query execution with list result."""
        # Mock enforcer to pass
        mock_enforcer.validate_sql.return_value = None
        # Mock rewriter to return same SQL
        mock_rewriter.rewrite_sql = AsyncMock(side_effect=lambda sql, tid: sql)
        mock_tool = AsyncMock()
        mock_tool.name = "execute_sql_query"
        mock_tool.ainvoke = AsyncMock(return_value=[1, 2, 3])

        mock_get_tools.return_value = [mock_tool]

        state = AgentState(
            messages=[],
            schema_context="",
            current_sql="SELECT 1 UNION SELECT 2 UNION SELECT 3",
            query_result=None,
            error=None,
            retry_count=0,
        )

        result = await validate_and_execute_node(state)

        assert result["query_result"] == [1, 2, 3]
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
        payload = json.dumps(
            {
                "rows": [{"id": 1}],
                "metadata": {"is_truncated": True, "row_limit": 100, "rows_returned": 1},
            }
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
    async def test_execute_backcompat_raw_rows(
        self, mock_rewriter, mock_enforcer, mock_get_tools, schema_fixture
    ):
        """Test backcompat when tool returns raw row list."""
        mock_enforcer.validate_sql.return_value = None
        mock_rewriter.rewrite_sql = AsyncMock(side_effect=lambda sql, tid: sql)
        mock_tool = AsyncMock()
        mock_tool.name = "execute_sql_query"
        mock_tool.ainvoke = AsyncMock(return_value=[{"id": 1}])

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
        assert result["result_is_truncated"] is False

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
        mock_tool.ainvoke = AsyncMock(return_value=json.dumps({"rows": [], "metadata": {}}))

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
        payload = json.dumps({"rows": [{"id": 1}], "columns": columns, "metadata": {}})
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
        mock_tool.ainvoke = AsyncMock(return_value=error_msg)

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
        mock_tool.ainvoke = AsyncMock(return_value="Database Error: syntax error at or near 'FROM'")

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
        payload = json.dumps(
            [{"error": "Database Error: syntax error", "error_category": "syntax"}]
        )
        mock_tool.ainvoke = AsyncMock(return_value=payload)

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

        assert result["error"] == "Connection timeout"
        assert result["query_result"] is None

    @pytest.mark.asyncio
    @patch("agent.nodes.execute.get_mcp_tools")
    @patch("agent.nodes.execute.PolicyEnforcer")
    @patch("agent.nodes.execute.TenantRewriter")
    async def test_validate_and_execute_node_invalid_json(
        self, mock_rewriter, mock_enforcer, mock_get_tools, schema_fixture
    ):
        """Test handling when result is string but not valid JSON."""
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

        # Should treat invalid JSON as error
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
