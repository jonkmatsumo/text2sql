"""SQL execution node for running validated queries with telemetry tracing."""

import logging

from agent.state import AgentState
from agent.telemetry import telemetry
from agent.telemetry_schema import SpanKind, TelemetryKeys
from agent.tools import get_mcp_tools
from agent.validation.policy_enforcer import PolicyEnforcer
from agent.validation.tenant_rewriter import TenantRewriter

logger = logging.getLogger(__name__)


async def validate_and_execute_node(state: AgentState) -> dict:
    """
    Node 3: ValidateAndExecute.

    Validates SQL against security policies, rewrites it for tenant isolation,
    and calls the 'execute_sql_query' MCP tool to execute the sanitized SQL.

    Args:
        state: Current agent state with current_sql populated

    Returns:
        dict: Updated state with query_result or error
    """
    with telemetry.start_span(
        name="execute_sql",
        span_type=SpanKind.AGENT_NODE,
    ) as span:
        span.set_attribute(TelemetryKeys.EVENT_TYPE, SpanKind.AGENT_NODE)
        span.set_attribute(TelemetryKeys.EVENT_NAME, "execute_sql")
        original_sql = state.get("current_sql")
        tenant_id = state.get("tenant_id")

        span.set_inputs(
            {
                "sql": original_sql,
                "tenant_id": tenant_id,
            }
        )

        if not original_sql:
            error = "No SQL query to execute"
            span.set_outputs({"error": error})
            return {"error": error, "query_result": None}

        # 1. Structural Validation (AST)
        try:
            PolicyEnforcer.validate_sql(original_sql)
        except ValueError as e:
            error = f"Security Policy Violation: {e}"
            logger.warning(f"Blocked unsafe SQL: {original_sql} | Reason: {e}")
            span.set_outputs({"error": error, "validation_failed": True})
            return {"error": error, "query_result": None}

        # 2. Tenant Isolation Rewriting
        try:
            # Inject RLS predicates (e.g. WHERE store_id = $1)
            rewritten_sql = await TenantRewriter.rewrite_sql(original_sql, tenant_id)

            # Audit Log
            logger.info(
                "SQL Audit",
                extra={
                    "tenant_id": tenant_id,
                    "original_sql": original_sql,
                    "rewritten_sql": rewritten_sql,
                    "event": "runtime_policy_enforcement",
                },
            )
            span.set_inputs({"rewritten_sql": rewritten_sql})

        except Exception as e:
            error = f"Policy Enforcement Failed: {e}"
            logger.error(f"Rewriting failed for: {original_sql} | Error: {e}")
            span.set_outputs({"error": error})
            return {"error": error, "query_result": None}

        try:
            tools = await get_mcp_tools()
            executor_tool = next((t for t in tools if t.name == "execute_sql_query"), None)

            if not executor_tool:
                error = "execute_sql_query tool not found in MCP server"
                span.set_outputs({"error": error})
                return {
                    "error": error,
                    "query_result": None,
                }

            # Execute via MCP Tool
            # Pass params only if the rewritten SQL contains placeholders (e.g. $1)
            # This prevents "server expects 0 arguments" errors for queries on public tables
            execute_params = [tenant_id] if (tenant_id and "$1" in rewritten_sql) else []

            result = await executor_tool.ainvoke(
                {
                    "sql_query": rewritten_sql,
                    "tenant_id": tenant_id,
                    "params": execute_params,
                }
            )

            # Check if the tool returned a database error string (simple case)
            if isinstance(result, str):
                if "Error:" in result or "Database Error:" in result:
                    span.set_outputs({"error": result})
                    return {"error": result, "query_result": None}

            # Use robust parsing utility
            from agent.utils.parsing import parse_tool_output

            parsed_data = parse_tool_output(result)

            if parsed_data:
                # Check for wrapped error object {"error": "..."}
                if (
                    isinstance(parsed_data, list)
                    and len(parsed_data) == 1
                    and isinstance(parsed_data[0], dict)
                    and "error" in parsed_data[0]
                ):
                    error_msg = parsed_data[0]["error"]
                    span.set_outputs({"error": error_msg})
                    return {"error": error_msg, "query_result": None}

                query_result = parsed_data
                error = None
            else:
                # Parsing failed or empty result. Check if it looks like an error string
                raw_str = str(result)
                if "Error:" in raw_str or "Database Error:" in raw_str:
                    error = raw_str
                    span.set_outputs({"error": error})
                    return {"error": error, "query_result": None}

                # Otherwise, assume it's just empty result set
                query_result = []
                error = None

            span.set_outputs(
                {
                    "result_count": len(query_result) if query_result else 0,
                    "success": True,
                }
            )

            # Cache successful SQL generation (if not from cache and tenant_id exists)
            # We cache even if result is empty, as long as execution was successful (no error)
            from_cache = state.get("from_cache", False)
            if not error and original_sql and tenant_id and not from_cache:
                try:
                    # Get cache update tool
                    cache_tool = next((t for t in tools if t.name == "update_cache"), None)
                    if cache_tool:
                        # Use the most recent user message as the cache key (G4 fix)
                        user_query = state["messages"][-1].content if state.get("messages") else ""
                        if user_query:
                            await cache_tool.ainvoke(
                                {
                                    "query": user_query,
                                    "sql": original_sql,
                                    "tenant_id": tenant_id,
                                }
                            )
                except Exception as e:
                    print(f"Warning: Cache update failed: {e}")

            return {"query_result": query_result, "error": error}

        except Exception as e:
            error = str(e)
            span.set_outputs({"error": error})
            return {
                "error": error,
                "query_result": None,
            }
