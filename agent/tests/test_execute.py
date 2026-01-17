"""Unit tests for SQL execution and validation node."""

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from agent_core.nodes.execute import validate_and_execute_node
from agent_core.state import AgentState


class TestValidateAndExecuteNode:
    """Unit tests for validate_and_execute_node function."""

    @pytest.mark.asyncio
    @patch("agent_core.nodes.execute.get_mcp_tools")
    @patch("agent_core.nodes.execute.PolicyEnforcer")
    @patch("agent_core.nodes.execute.TenantRewriter")
    async def test_validate_and_execute_node_success_json_string(
        self, mock_rewriter, mock_enforcer, mock_get_tools
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
            current_sql="SELECT COUNT(*) as count FROM film",
            query_result=None,
            error=None,
            retry_count=0,
            tenant_id=None,  # Explicitly set to None for this test
        )

        result = await validate_and_execute_node(state)

        # Verify tool was called with correct query (tenant_id is extracted from context)
        mock_tool.ainvoke.assert_called_once()
        call_args = mock_tool.ainvoke.call_args[0][0]
        assert call_args["sql_query"] == "SELECT COUNT(*) as count FROM film"
        assert call_args["tenant_id"] is None  # Mocks state tenant_id which is None here

        # Verify result was parsed and returned
        assert result["query_result"] == [{"count": 1000}]
        assert result["error"] is None

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    @patch("agent_core.nodes.execute.get_mcp_tools")
    @patch("agent_core.nodes.execute.PolicyEnforcer")
    @patch("agent_core.nodes.execute.TenantRewriter")
    async def test_validate_and_execute_node_success_dict_result(
        self, mock_rewriter, mock_enforcer, mock_get_tools
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
            current_sql="SELECT * FROM film LIMIT 1",
            query_result=None,
            error=None,
            retry_count=0,
        )

        result = await validate_and_execute_node(state)

        assert result["query_result"] == [{"id": 1, "title": "Film 1"}]
        assert result["error"] is None

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    @patch("agent_core.nodes.execute.get_mcp_tools")
    @patch("agent_core.nodes.execute.PolicyEnforcer")
    @patch("agent_core.nodes.execute.TenantRewriter")
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
    @pytest.mark.asyncio
    @patch("agent_core.nodes.execute.get_mcp_tools")
    @patch("agent_core.nodes.execute.PolicyEnforcer")
    @patch("agent_core.nodes.execute.TenantRewriter")
    async def test_validate_and_execute_node_error_string(
        self, mock_rewriter, mock_enforcer, mock_get_tools
    ):
        """Test error handling when tool returns error string."""
        # Mock enforcer to pass
        mock_enforcer.validate_sql.return_value = None
        # Mock rewriter to return same SQL
        mock_rewriter.rewrite_sql = AsyncMock(side_effect=lambda sql, tid: sql)
        mock_tool = AsyncMock()
        mock_tool.name = "execute_sql_query"
        mock_tool.ainvoke = AsyncMock(return_value="Error: relation 'films' does not exist")

        mock_get_tools.return_value = [mock_tool]

        state = AgentState(
            messages=[],
            schema_context="",
            current_sql="SELECT * FROM films",
            query_result=None,
            error=None,
            retry_count=0,
        )

        result = await validate_and_execute_node(state)

        assert result["error"] == "Error: relation 'films' does not exist"
        assert result["query_result"] is None

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    @patch("agent_core.nodes.execute.get_mcp_tools")
    @patch("agent_core.nodes.execute.PolicyEnforcer")
    @patch("agent_core.nodes.execute.TenantRewriter")
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
    @patch("agent_core.nodes.execute.get_mcp_tools")
    @patch("agent_core.nodes.execute.PolicyEnforcer")
    @patch("agent_core.nodes.execute.TenantRewriter")
    async def test_validate_and_execute_node_missing_tool(
        self, mock_rewriter, mock_enforcer, mock_get_tools
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
            current_sql="SELECT * FROM film",
            query_result=None,
            error=None,
            retry_count=0,
        )

        result = await validate_and_execute_node(state)

        assert result["error"] == "execute_sql_query tool not found in MCP server"
        assert result["query_result"] is None

    @pytest.mark.asyncio
    @patch("agent_core.nodes.execute.get_mcp_tools")
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
    @patch("agent_core.nodes.execute.get_mcp_tools")
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
    @patch("agent_core.nodes.execute.get_mcp_tools")
    @patch("agent_core.nodes.execute.PolicyEnforcer")
    @patch("agent_core.nodes.execute.TenantRewriter")
    async def test_validate_and_execute_node_execution_exception(
        self, mock_rewriter, mock_enforcer, mock_get_tools
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
            current_sql="SELECT * FROM film",
            query_result=None,
            error=None,
            retry_count=0,
        )

        result = await validate_and_execute_node(state)

        assert result["error"] == "Connection timeout"
        assert result["query_result"] is None

    @pytest.mark.asyncio
    @patch("agent_core.nodes.execute.get_mcp_tools")
    @patch("agent_core.nodes.execute.PolicyEnforcer")
    @patch("agent_core.nodes.execute.TenantRewriter")
    async def test_validate_and_execute_node_invalid_json(
        self, mock_rewriter, mock_enforcer, mock_get_tools
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
            current_sql="SELECT * FROM film",
            query_result=None,
            error=None,
            retry_count=0,
        )

        result = await validate_and_execute_node(state)

        # Should treat invalid JSON as error
        assert result["error"] == "Error: Invalid JSON string"
        assert result["query_result"] is None

    @pytest.mark.asyncio
    @patch("agent_core.nodes.execute.get_mcp_tools")
    @patch("agent_core.nodes.execute.PolicyEnforcer")
    @patch("agent_core.nodes.execute.TenantRewriter")
    async def test_validate_and_execute_node_empty_tools(
        self, mock_rewriter, mock_enforcer, mock_get_tools
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
            current_sql="SELECT * FROM film",
            query_result=None,
            error=None,
            retry_count=0,
        )

        result = await validate_and_execute_node(state)

        assert result["error"] == "execute_sql_query tool not found in MCP server"
        assert result["query_result"] is None

    @pytest.mark.asyncio
    @patch("agent_core.nodes.execute.PolicyEnforcer")
    async def test_validate_and_execute_node_blocks_unsafe_sql(self, mock_enforcer):
        """Test that PolicyEnforcer blocks unsafe SQL before tool invocation."""
        mock_enforcer.validate_sql.side_effect = ValueError("Access to table 'film' is not allowed")

        state = AgentState(
            messages=[],
            schema_context="",
            current_sql="SELECT * FROM film",
            query_result=None,
            error=None,
            retry_count=0,
        )

        result = await validate_and_execute_node(state)

        assert "Security Policy Violation" in result["error"]
        assert "Access to table 'film' is not allowed" in result["error"]
        assert result["query_result"] is None
