"""SQL execution node for running validated queries with telemetry tracing."""

import logging
import re
import time

from agent.state import AgentState
from agent.state.result_completeness import ResultCompleteness
from agent.telemetry import telemetry
from agent.telemetry_schema import SpanKind, TelemetryKeys
from agent.tools import get_mcp_tools
from agent.validation.policy_enforcer import PolicyEnforcer
from agent.validation.tenant_rewriter import TenantRewriter
from common.config.env import get_env_bool

logger = logging.getLogger(__name__)


class ToolResponseMalformedError(RuntimeError):
    """Raised when execute_sql_query returns an unexpected payload."""


def _extract_missing_identifiers(error_text: str) -> list[str]:
    patterns = [
        r'relation "(?P<name>[^"]+)" does not exist',
        r'table "(?P<name>[^"]+)" does not exist',
        r'column "(?P<name>[^"]+)" does not exist',
        r"no such table: (?P<name>[\w\.]+)",
        r"unknown column (?P<name>[\w\.]+)",
    ]
    identifiers = []
    for pattern in patterns:
        for match in re.finditer(pattern, error_text, flags=re.IGNORECASE):
            name = match.group("name")
            if name and name not in identifiers:
                identifiers.append(name)
    return identifiers


def _schema_drift_hint(error_text: str) -> tuple[bool, list[str]]:
    identifiers = _extract_missing_identifiers(error_text)
    return (len(identifiers) > 0, identifiers)


def parse_execute_tool_response(payload) -> dict:
    """Parse execute_sql_query response into a normalized shape."""
    from agent.utils.parsing import parse_tool_output

    def _payload_preview(value) -> str:
        preview = "" if value is None else str(value)
        return preview[:500]

    def _payload_type(value) -> str:
        if value is None:
            return "null"
        return type(value).__name__

    def _looks_like_json_object(value) -> bool:
        if not isinstance(value, str):
            return False
        stripped = value.strip()
        return stripped.startswith("{") and stripped.endswith("}")

    def _is_message_wrapper(value) -> bool:
        return isinstance(value, dict) and any(key in value for key in ("type", "content", "text"))

    diagnostics = {
        "payload_type": _payload_type(payload),
        "payload_preview": _payload_preview(payload),
    }
    if payload is None:
        return {"response_shape": "malformed", "diagnostics": diagnostics}

    dict_payload = (
        isinstance(payload, dict) and not _is_message_wrapper(payload)
    ) or _looks_like_json_object(payload)
    parsed = parse_tool_output(payload)
    response_shape = "legacy"
    if isinstance(parsed, list) and len(parsed) == 1:
        item = parsed[0]
        if isinstance(item, dict):
            if "error" in item:
                return {
                    "response_shape": "error",
                    "error": item.get("error"),
                    "error_category": item.get("error_category"),
                    "required_capability": item.get("required_capability"),
                    "provider": item.get("provider"),
                }
            if "rows" in item and "metadata" in item:
                return {
                    "response_shape": "enveloped",
                    "rows": item.get("rows") or [],
                    "metadata": item.get("metadata") or {},
                    "columns": item.get("columns"),
                }
            if dict_payload:
                return {"response_shape": "malformed", "diagnostics": diagnostics}
            return {"response_shape": "legacy", "rows": parsed}
        if isinstance(item, str) and (
            "Error:" in item or "Database Error:" in item or "Execution Error:" in item
        ):
            return {"response_shape": "error", "error": item, "error_category": None}
        return {"response_shape": "malformed", "diagnostics": diagnostics}
    if isinstance(parsed, list):
        return {"response_shape": "legacy", "rows": parsed}
    elif isinstance(parsed, dict):
        if "error" in parsed:
            return {
                "response_shape": "error",
                "error": parsed.get("error"),
                "error_category": parsed.get("error_category"),
                "required_capability": parsed.get("required_capability"),
                "provider": parsed.get("provider"),
            }
        response_shape = "malformed"
    else:
        response_shape = "malformed"

    if response_shape == "malformed":
        return {"response_shape": response_shape, "diagnostics": diagnostics}
    return {"response_shape": response_shape}


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
            remaining = None
            deadline_ts = state.get("deadline_ts")
            if deadline_ts is not None:
                remaining = max(0.0, deadline_ts - time.monotonic())
                span.set_attribute("deadline.propagated", True)
                span.set_attribute("deadline.remaining_seconds", remaining)
                if remaining < 0.5:
                    error = "Execution timed out before query could start."
                    span.set_attribute("timeout.triggered", True)
                    span.set_outputs({"error": error})
                    return {
                        "error": error,
                        "error_category": "timeout",
                        "query_result": None,
                    }

            result = await executor_tool.ainvoke(
                {
                    "sql_query": rewritten_sql,
                    "tenant_id": tenant_id,
                    "params": execute_params,
                    "include_columns": True,
                    "timeout_seconds": remaining,
                    "page_token": state.get("page_token"),
                    "page_size": state.get("page_size"),
                }
            )

            parsed = parse_execute_tool_response(result)
            result_is_truncated = None
            result_row_limit = None
            result_rows_returned = None
            result_columns = None
            result_next_page_token = None
            result_page_size = None
            result_partial_reason = None

            def _maybe_add_schema_drift(error_msg: str) -> dict:
                if not get_env_bool("AGENT_SCHEMA_DRIFT_HINTS", True):
                    return {}
                suspected, identifiers = _schema_drift_hint(error_msg)
                if not suspected:
                    return {}
                auto_refresh = get_env_bool("AGENT_SCHEMA_DRIFT_AUTO_REFRESH", False)
                span.set_attribute("schema.drift.suspected", True)
                span.set_attribute("schema.drift.missing_identifiers_count", len(identifiers))
                span.set_attribute("schema.drift.auto_refresh_enabled", auto_refresh)
                return {
                    "schema_drift_suspected": True,
                    "missing_identifiers": identifiers,
                    "schema_snapshot_id": state.get("schema_snapshot_id"),
                    "schema_drift_auto_refresh": auto_refresh,
                }

            span.set_attribute("tool.response_shape", parsed.get("response_shape"))
            if parsed.get("response_shape") == "error":
                error_msg = parsed.get("error") or "Tool returned an error."
                error_category = parsed.get("error_category")
                required_capability = parsed.get("required_capability")
                provider = parsed.get("provider")
                span.set_outputs(
                    {
                        "error": error_msg,
                        "error_category": error_category,
                    }
                )
                if error_category:
                    span.set_attribute("error_category", error_category)
                    span.set_attribute("timeout.triggered", error_category == "timeout")
                if error_category == "unsupported_capability":
                    if required_capability:
                        error_msg = (
                            "This backend does not support "
                            f"{required_capability} for this request."
                        )
                    else:
                        error_msg = (
                            "This backend does not support a required capability "
                            "for this request."
                        )
                    if required_capability:
                        span.set_attribute("error.required_capability", required_capability)
                    if provider:
                        span.set_attribute("error.provider", provider)
                drift_hint = _maybe_add_schema_drift(error_msg)
                return {
                    "error": error_msg,
                    "query_result": None,
                    "error_category": error_category,
                    "error_metadata": (
                        {
                            "required_capability": required_capability,
                            "provider": provider,
                        }
                        if error_category == "unsupported_capability"
                        else None
                    ),
                    **drift_hint,
                }

            if parsed.get("response_shape") == "malformed":
                diagnostics = parsed.get("diagnostics") or {}
                payload_type = diagnostics.get("payload_type")
                payload_preview = diagnostics.get("payload_preview")
                if payload_type:
                    span.set_attribute("tool.malformed.payload_type", payload_type)
                if payload_preview:
                    span.set_attribute("tool.malformed.payload_preview", payload_preview)

                trace_id = telemetry.get_current_trace_id()
                if trace_id:
                    error_msg = f"Tool response malformed. Trace ID: {trace_id}."
                else:
                    error_msg = "Tool response malformed. Trace ID unavailable."

                span.set_outputs({"error": error_msg})
                span.set_attribute("tool.response_shape", "malformed")
                return {
                    "error": error_msg,
                    "error_category": "tool_response_malformed",
                    "query_result": None,
                }

            if parsed.get("response_shape") == "enveloped":
                query_result = parsed.get("rows") or []
                metadata = parsed.get("metadata") or {}
                result_is_truncated = metadata.get("is_truncated")
                result_row_limit = metadata.get("row_limit")
                result_rows_returned = metadata.get("rows_returned")
                result_next_page_token = metadata.get("next_page_token")
                result_page_size = metadata.get("page_size")
                result_partial_reason = metadata.get("partial_reason")
                result_columns = parsed.get("columns")
                error = None
            else:
                query_result = parsed.get("rows") or []
                error = None

            span.set_outputs(
                {
                    "result_count": len(query_result) if query_result else 0,
                    "success": True,
                }
            )
            span.set_attribute("result.is_truncated", bool(result_is_truncated))
            if result_row_limit is not None:
                span.set_attribute("result.row_limit", result_row_limit)
            if result_rows_returned is not None:
                span.set_attribute("result.rows_returned", result_rows_returned)
            span.set_attribute("result.columns_available", bool(result_columns))
            span.set_attribute("timeout.triggered", False)

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
                                    "schema_snapshot_id": state.get("schema_snapshot_id"),
                                }
                            )
                except Exception:
                    logger.warning("Cache update failed", exc_info=True)

            rows_returned = (
                result_rows_returned
                if result_rows_returned is not None
                else (len(query_result) if query_result else 0)
            )
            is_limited = bool(state.get("result_is_limited"))
            query_limit = state.get("result_limit") if is_limited else None
            return {
                "query_result": query_result,
                "error": error,
                "result_is_truncated": result_is_truncated or False,
                "result_row_limit": result_row_limit,
                "result_rows_returned": (rows_returned),
                "result_columns": result_columns,
                "result_completeness": ResultCompleteness.from_parts(
                    rows_returned=rows_returned,
                    is_truncated=bool(result_is_truncated),
                    is_limited=is_limited,
                    row_limit=result_row_limit,
                    query_limit=query_limit,
                    next_page_token=result_next_page_token,
                    page_size=result_page_size,
                    partial_reason=result_partial_reason,
                ).to_dict(),
            }

        except Exception as e:
            error = str(e)
            span.set_outputs({"error": error})
            return {
                "error": error,
                "query_result": None,
            }
