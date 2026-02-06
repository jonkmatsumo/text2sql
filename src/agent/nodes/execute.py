"""SQL execution node for running validated queries with telemetry tracing."""

import logging
import time

from agent.state import AgentState
from agent.state.result_completeness import ResultCompleteness
from agent.telemetry import telemetry
from agent.telemetry_schema import SpanKind, TelemetryKeys
from agent.tools import get_mcp_tools
from agent.utils.pagination_prefetch import (
    build_prefetch_cache_key,
    get_prefetch_config,
    pop_prefetched_page,
    start_prefetch_task,
)
from agent.validation.policy_enforcer import PolicyEnforcer
from agent.validation.tenant_rewriter import TenantRewriter
from common.config.env import get_env_bool, get_env_int, get_env_str
from dal.error_patterns import extract_missing_identifiers

logger = logging.getLogger(__name__)


class ToolResponseMalformedError(RuntimeError):
    """Raised when execute_sql_query returns an unexpected payload."""


def _schema_drift_hint(error_text: str, provider: str) -> tuple[bool, list[str]]:
    identifiers = extract_missing_identifiers(provider, error_text)
    return (len(identifiers) > 0, identifiers)


def _safe_env_int(name: str, default: int, minimum: int) -> int:
    try:
        parsed = get_env_int(name, default)
    except ValueError:
        logger.warning("Invalid %s; using default %s", name, default)
        return default
    if parsed is None:
        return default
    return max(minimum, int(parsed))


def _auto_pagination_config() -> tuple[bool, int, int]:
    mode = (get_env_str("AGENT_AUTO_PAGINATION", "off") or "off").strip().lower()
    enabled = mode == "on"
    max_pages = _safe_env_int("AGENT_AUTO_PAGINATION_MAX_PAGES", default=3, minimum=1)
    max_rows = _safe_env_int("AGENT_AUTO_PAGINATION_MAX_ROWS", default=5000, minimum=1)
    return enabled, max_pages, max_rows


def _is_prefetch_candidate(latency_seconds: float, rows_returned: int, page_size: int) -> bool:
    """Conservative guard to keep prefetch cost predictable."""
    if latency_seconds > 1.0:
        return False
    if page_size <= 0:
        return False
    if rows_returned > page_size * 2:
        return False
    return True


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
                    "retry_after_seconds": item.get("retry_after_seconds"),
                    "required_capability": item.get("required_capability"),
                    "capability_required": item.get("capability_required"),
                    "capability_supported": item.get("capability_supported"),
                    "fallback_applied": item.get("fallback_applied"),
                    "fallback_mode": item.get("fallback_mode"),
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
                "retry_after_seconds": parsed.get("retry_after_seconds"),
                "required_capability": parsed.get("required_capability"),
                "capability_required": parsed.get("capability_required"),
                "capability_supported": parsed.get("capability_supported"),
                "fallback_applied": parsed.get("fallback_applied"),
                "fallback_mode": parsed.get("fallback_mode"),
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
            logger.error(
                f"Rewriting failed for: {original_sql} | Error: {e}",
                extra={"error_type": type(e).__name__},
            )
            span.set_outputs({"error": error})
            span.set_attribute("error.type", type(e).__name__)
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

            # Pre-execution schema validation hook
            from agent.utils.schema_fingerprint import validate_sql_against_schema

            schema_context = state.get("raw_schema_context") or []
            pre_exec_passed, missing_tables, pre_exec_warning = validate_sql_against_schema(
                rewritten_sql, schema_context
            )
            span.set_attribute("validation.pre_exec_check_passed", pre_exec_passed)
            if not pre_exec_passed:
                span.set_attribute("validation.pre_exec_missing_tables", len(missing_tables))
                if pre_exec_warning:
                    logger.warning(pre_exec_warning)
                    span.add_event("validation.pre_exec_warning", {"message": pre_exec_warning})

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

            execute_payload = {
                "sql_query": rewritten_sql,
                "tenant_id": tenant_id,
                "params": execute_params,
                "include_columns": True,
                "timeout_seconds": remaining,
                "page_token": state.get("page_token"),
                "page_size": state.get("page_size"),
            }
            interactive_session = bool(state.get("interactive_session"))
            prefetch_enabled, prefetch_max_concurrency, prefetch_reason = get_prefetch_config(
                interactive_session
            )
            seed_value = state.get("seed")
            seed = seed_value if isinstance(seed_value, int) else None
            existing_completeness = state.get("result_completeness")
            completeness_hint = None
            if isinstance(existing_completeness, dict):
                completeness_hint = existing_completeness.get("partial_reason")

            first_page_started_at = time.monotonic()
            prefetched_cache_key = None
            page_token = execute_payload.get("page_token")
            if prefetch_enabled and page_token:
                prefetched_cache_key = build_prefetch_cache_key(
                    sql_query=rewritten_sql,
                    tenant_id=tenant_id,
                    page_token=str(page_token),
                    page_size=execute_payload.get("page_size"),
                    schema_snapshot_id=state.get("schema_snapshot_id"),
                    seed=seed,
                    completeness_hint=completeness_hint,
                )
                prefetched_payload = pop_prefetched_page(prefetched_cache_key)
                if prefetched_payload is not None:
                    span.add_event(
                        "pagination.prefetch_cache_hit",
                        {"cache_key": prefetched_cache_key[:12]},
                    )
                    result = prefetched_payload
                    prefetch_reason = "cache_hit"
                else:
                    result = await executor_tool.ainvoke(execute_payload)
                    prefetch_reason = "cache_miss"
            else:
                result = await executor_tool.ainvoke(execute_payload)
            first_page_latency_seconds = max(0.0, time.monotonic() - first_page_started_at)

            parsed = parse_execute_tool_response(result)
            result_is_truncated = None
            result_row_limit = None
            result_rows_returned = None
            result_columns = None
            result_next_page_token = None
            result_page_size = None
            result_partial_reason = None
            result_capability_required = None
            result_capability_supported = None
            result_fallback_applied = None
            result_fallback_mode = None
            result_cap_detected = None
            result_cap_mitigation_applied = None
            result_cap_mitigation_mode = None
            result_auto_paginated = False
            result_pages_fetched = 1
            result_auto_pagination_stopped_reason = "disabled"
            result_prefetch_enabled = prefetch_enabled
            result_prefetch_scheduled = False
            result_prefetch_reason = prefetch_reason

            def _maybe_add_schema_drift(error_msg: str) -> dict:
                if not get_env_bool("AGENT_SCHEMA_DRIFT_HINTS", True):
                    return {}
                provider = get_env_str(
                    "QUERY_TARGET_BACKEND", get_env_str("QUERY_TARGET_PROVIDER", "postgres")
                )
                suspected, identifiers = _schema_drift_hint(error_msg, provider)
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
                retry_after_seconds = parsed.get("retry_after_seconds")
                required_capability = parsed.get("required_capability")
                capability_required = parsed.get("capability_required")
                capability_supported = parsed.get("capability_supported")
                fallback_applied = parsed.get("fallback_applied")
                fallback_mode = parsed.get("fallback_mode")
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
                if retry_after_seconds is not None:
                    span.set_attribute("retry.retry_after_seconds", float(retry_after_seconds))
                if error_category == "unsupported_capability":
                    if capability_required and not required_capability:
                        required_capability = capability_required
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
                    if capability_supported is not None:
                        span.set_attribute("error.capability_supported", bool(capability_supported))
                    if fallback_applied is not None:
                        span.set_attribute("error.fallback_applied", bool(fallback_applied))
                    if fallback_mode:
                        span.set_attribute("error.fallback_mode", str(fallback_mode))
                    if provider:
                        span.set_attribute("error.provider", provider)
                drift_hint = _maybe_add_schema_drift(error_msg)
                return {
                    "error": error_msg,
                    "query_result": None,
                    "error_category": error_category,
                    "retry_after_seconds": retry_after_seconds,
                    "error_metadata": (
                        {
                            "required_capability": required_capability,
                            "capability_required": capability_required,
                            "capability_supported": capability_supported,
                            "fallback_applied": fallback_applied,
                            "fallback_mode": fallback_mode,
                            "retry_after_seconds": retry_after_seconds,
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
                result_capability_required = metadata.get("capability_required")
                result_capability_supported = metadata.get("capability_supported")
                result_fallback_applied = metadata.get("fallback_applied")
                result_fallback_mode = metadata.get("fallback_mode")
                result_cap_detected = metadata.get("cap_detected")
                result_cap_mitigation_applied = metadata.get("cap_mitigation_applied")
                result_cap_mitigation_mode = metadata.get("cap_mitigation_mode")
                auto_pagination_enabled, auto_max_pages, auto_max_rows = _auto_pagination_config()
                if auto_pagination_enabled:
                    result_auto_pagination_stopped_reason = "no_next_page"
                if auto_pagination_enabled and len(query_result) > auto_max_rows:
                    query_result = query_result[:auto_max_rows]
                    result_is_truncated = True
                    if not result_partial_reason:
                        result_partial_reason = "LIMITED"
                    result_rows_returned = len(query_result)
                    result_auto_pagination_stopped_reason = "max_rows"
                elif (
                    auto_pagination_enabled
                    and result_next_page_token
                    and not (
                        result_capability_required == "pagination"
                        and result_capability_supported is False
                    )
                ):
                    result_auto_paginated = True
                    aggregated_rows = list(query_result)
                    next_page_token = result_next_page_token
                    while next_page_token:
                        if result_pages_fetched >= auto_max_pages:
                            result_auto_pagination_stopped_reason = "max_pages"
                            break
                        if len(aggregated_rows) >= auto_max_rows:
                            result_auto_pagination_stopped_reason = "max_rows"
                            break

                        page_timeout = None
                        if deadline_ts is not None:
                            page_timeout = max(0.0, deadline_ts - time.monotonic())
                            if page_timeout <= 0.0:
                                result_auto_pagination_stopped_reason = "budget_exhausted"
                                break

                        page_payload = {
                            **execute_payload,
                            "page_token": next_page_token,
                            "page_size": execute_payload.get("page_size") or result_page_size,
                            "timeout_seconds": page_timeout,
                        }
                        page_result = await executor_tool.ainvoke(page_payload)
                        page_parsed = parse_execute_tool_response(page_result)
                        span.add_event(
                            "pagination.auto_page_fetch",
                            {
                                "page_number": result_pages_fetched + 1,
                                "has_next_page_token": bool(next_page_token),
                                "max_pages": auto_max_pages,
                                "max_rows": auto_max_rows,
                            },
                        )
                        if page_parsed.get("response_shape") != "enveloped":
                            result_auto_pagination_stopped_reason = "non_enveloped_response"
                            break

                        page_rows = page_parsed.get("rows") or []
                        page_metadata = page_parsed.get("metadata") or {}
                        aggregated_rows.extend(page_rows)
                        result_pages_fetched += 1

                        next_page_token = page_metadata.get("next_page_token")
                        result_next_page_token = next_page_token
                        result_is_truncated = bool(result_is_truncated) or bool(
                            page_metadata.get("is_truncated")
                        )

                        page_row_limit = page_metadata.get("row_limit")
                        if page_row_limit is not None:
                            result_row_limit = page_row_limit

                        page_rows_returned = page_metadata.get("rows_returned")
                        if page_rows_returned is not None:
                            result_rows_returned = page_rows_returned

                        page_page_size = page_metadata.get("page_size")
                        if page_page_size is not None:
                            result_page_size = page_page_size

                        page_partial_reason = page_metadata.get("partial_reason")
                        if page_partial_reason:
                            result_partial_reason = page_partial_reason

                        page_capability_required = page_metadata.get("capability_required")
                        if page_capability_required is not None:
                            result_capability_required = page_capability_required

                        page_capability_supported = page_metadata.get("capability_supported")
                        if page_capability_supported is not None:
                            result_capability_supported = page_capability_supported

                        page_fallback_applied = page_metadata.get("fallback_applied")
                        if page_fallback_applied is not None:
                            result_fallback_applied = page_fallback_applied

                        page_fallback_mode = page_metadata.get("fallback_mode")
                        if page_fallback_mode:
                            result_fallback_mode = page_fallback_mode

                        page_cap_detected = page_metadata.get("cap_detected")
                        if page_cap_detected is not None:
                            result_cap_detected = bool(result_cap_detected) or bool(
                                page_cap_detected
                            )

                        page_cap_mitigation_applied = page_metadata.get("cap_mitigation_applied")
                        if page_cap_mitigation_applied is not None:
                            result_cap_mitigation_applied = bool(
                                result_cap_mitigation_applied
                            ) or bool(page_cap_mitigation_applied)

                        page_cap_mitigation_mode = page_metadata.get("cap_mitigation_mode")
                        if page_cap_mitigation_mode:
                            result_cap_mitigation_mode = page_cap_mitigation_mode

                        if len(aggregated_rows) >= auto_max_rows:
                            aggregated_rows = aggregated_rows[:auto_max_rows]
                            result_is_truncated = True
                            if not result_partial_reason:
                                result_partial_reason = "LIMITED"
                            result_auto_pagination_stopped_reason = "max_rows"
                            break
                        if not next_page_token:
                            result_auto_pagination_stopped_reason = "no_next_page"

                    query_result = aggregated_rows
                    result_rows_returned = len(query_result)
                    result_auto_paginated = result_pages_fetched > 1
                elif (
                    auto_pagination_enabled
                    and result_capability_required == "pagination"
                    and result_capability_supported is False
                ):
                    result_auto_pagination_stopped_reason = "unsupported_capability"

                if prefetch_enabled:
                    if result_prefetch_reason == "cache_hit":
                        pass
                    elif auto_pagination_enabled:
                        result_prefetch_reason = "auto_pagination_enabled"
                    elif not result_next_page_token:
                        result_prefetch_reason = "no_next_page"
                    else:
                        prefetch_page_size = execute_payload.get("page_size") or result_page_size
                        if prefetch_page_size is None:
                            prefetch_page_size = 0
                        first_page_rows = int(result_rows_returned or len(query_result))
                        if not _is_prefetch_candidate(
                            first_page_latency_seconds,
                            first_page_rows,
                            int(prefetch_page_size),
                        ):
                            result_prefetch_reason = "not_cheap"
                        else:
                            prefetch_timeout = None
                            if deadline_ts is not None:
                                prefetch_timeout = max(
                                    0.0, min(2.0, deadline_ts - time.monotonic())
                                )
                                if prefetch_timeout <= 0.0:
                                    result_prefetch_reason = "budget_exhausted"
                            if result_prefetch_reason != "budget_exhausted":
                                next_page_token_for_prefetch = str(result_next_page_token)
                                prefetch_key = build_prefetch_cache_key(
                                    sql_query=rewritten_sql,
                                    tenant_id=tenant_id,
                                    page_token=next_page_token_for_prefetch,
                                    page_size=int(prefetch_page_size),
                                    schema_snapshot_id=state.get("schema_snapshot_id"),
                                    seed=seed,
                                    completeness_hint=result_partial_reason,
                                )

                                async def _fetch_prefetched_page() -> dict | None:
                                    prefetch_payload = {
                                        **execute_payload,
                                        "page_token": next_page_token_for_prefetch,
                                        "page_size": int(prefetch_page_size),
                                        "timeout_seconds": prefetch_timeout,
                                    }
                                    prefetched_raw = await executor_tool.ainvoke(prefetch_payload)
                                    prefetched_parsed = parse_execute_tool_response(prefetched_raw)
                                    if prefetched_parsed.get("response_shape") != "enveloped":
                                        return None
                                    page_payload = {
                                        "rows": prefetched_parsed.get("rows") or [],
                                        "metadata": prefetched_parsed.get("metadata") or {},
                                    }
                                    columns = prefetched_parsed.get("columns")
                                    if columns is not None:
                                        page_payload["columns"] = columns
                                    return page_payload

                                result_prefetch_scheduled = start_prefetch_task(
                                    prefetch_key,
                                    _fetch_prefetched_page,
                                    max_concurrency=prefetch_max_concurrency,
                                )
                                if result_prefetch_scheduled:
                                    result_prefetch_reason = "scheduled"
                                else:
                                    result_prefetch_reason = "already_cached_or_inflight"
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
            else:
                # Ensure rows_returned is always set (contract requirement)
                span.set_attribute("result.rows_returned", len(query_result) if query_result else 0)
            span.set_attribute("result.columns_available", bool(result_columns))
            span.set_attribute("timeout.triggered", False)
            # Truncation contract: always set partial_reason for debugging
            if result_partial_reason:
                span.set_attribute("result.partial_reason", result_partial_reason)
            is_limited = bool(state.get("result_is_limited"))
            span.set_attribute("result.is_limited", is_limited)
            if result_capability_required:
                span.set_attribute("capability.required", result_capability_required)
            if result_capability_supported is not None:
                span.set_attribute("capability.supported", bool(result_capability_supported))
            if result_fallback_applied is not None:
                span.set_attribute("capability.fallback_applied", bool(result_fallback_applied))
            if result_fallback_mode:
                span.set_attribute("capability.fallback_mode", str(result_fallback_mode))
            if result_cap_detected is not None:
                span.set_attribute("result.cap_detected", bool(result_cap_detected))
            if result_cap_mitigation_applied is not None:
                span.set_attribute(
                    "result.cap_mitigation_applied", bool(result_cap_mitigation_applied)
                )
            if result_cap_mitigation_mode:
                span.set_attribute("result.cap_mitigation_mode", str(result_cap_mitigation_mode))
            span.set_attribute("pagination.auto_paginated", bool(result_auto_paginated))
            span.set_attribute("pagination.pages_fetched", int(result_pages_fetched))
            if result_auto_pagination_stopped_reason:
                span.set_attribute(
                    "pagination.auto_stopped_reason", str(result_auto_pagination_stopped_reason)
                )
            span.set_attribute("prefetch.enabled", bool(result_prefetch_enabled))
            span.set_attribute("prefetch.scheduled", bool(result_prefetch_scheduled))
            span.set_attribute("prefetch.max_concurrency", int(prefetch_max_concurrency))
            if result_prefetch_reason:
                span.set_attribute("prefetch.reason", str(result_prefetch_reason))

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
                "result_cap_detected": bool(result_cap_detected),
                "result_cap_mitigation_applied": bool(result_cap_mitigation_applied),
                "result_cap_mitigation_mode": result_cap_mitigation_mode,
                "result_auto_paginated": bool(result_auto_paginated),
                "result_pages_fetched": int(result_pages_fetched),
                "result_auto_pagination_stopped_reason": result_auto_pagination_stopped_reason,
                "result_prefetch_enabled": bool(result_prefetch_enabled),
                "result_prefetch_scheduled": bool(result_prefetch_scheduled),
                "result_prefetch_reason": result_prefetch_reason,
                "retry_after_seconds": None,
                "result_completeness": ResultCompleteness.from_parts(
                    rows_returned=rows_returned,
                    is_truncated=bool(result_is_truncated),
                    is_limited=is_limited,
                    row_limit=result_row_limit,
                    query_limit=query_limit,
                    next_page_token=result_next_page_token,
                    page_size=result_page_size,
                    partial_reason=result_partial_reason,
                    cap_detected=bool(result_cap_detected),
                    cap_mitigation_applied=bool(result_cap_mitigation_applied),
                    cap_mitigation_mode=result_cap_mitigation_mode,
                    auto_paginated=bool(result_auto_paginated),
                    pages_fetched=int(result_pages_fetched),
                    auto_pagination_stopped_reason=result_auto_pagination_stopped_reason,
                    prefetch_enabled=bool(result_prefetch_enabled),
                    prefetch_scheduled=bool(result_prefetch_scheduled),
                    prefetch_reason=result_prefetch_reason,
                ).to_dict(),
            }

        except Exception as e:
            error = str(e)
            span.set_outputs({"error": error})
            span.set_attribute("error.type", type(e).__name__)
            return {
                "error": error,
                "query_result": None,
            }
