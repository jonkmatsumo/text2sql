"""SQL execution node for running validated queries with telemetry tracing."""

import logging
import time
from typing import Optional

from agent.replay_bundle import lookup_replay_tool_output
from agent.state import AgentState
from agent.state.result_completeness import ResultCompleteness
from agent.telemetry import telemetry
from agent.telemetry_schema import SpanKind, TelemetryKeys
from agent.tools import get_mcp_tools
from agent.utils.pagination_prefetch import (
    PrefetchManager,
    build_prefetch_cache_key,
    get_prefetch_config,
    pop_prefetched_page,
)
from agent.validation.policy_enforcer import PolicyEnforcer
from agent.validation.tenant_rewriter import TenantRewriter
from common.config.env import get_env_bool, get_env_int, get_env_str
from common.constants.reason_codes import (
    DriftDetectionMethod,
    PaginationStopReason,
    PrefetchSuppressionReason,
)
from common.models.tool_envelopes import (
    ExecuteSQLQueryMetadata,
    ExecuteSQLQueryResponseEnvelope,
    parse_execute_sql_response,
)
from common.utils.decisions import format_decision_summary

logger = logging.getLogger(__name__)


class ToolResponseMalformedError(RuntimeError):
    """Raised when execute_sql_query returns an unexpected payload."""


def _parse_tool_response_with_shim(payload) -> ExecuteSQLQueryResponseEnvelope:
    """Parse tool response with optional legacy shim."""
    envelope = parse_execute_sql_response(payload)

    # Check if we should attempt legacy list shim
    is_candidate_for_shim = (
        envelope.is_error()
        and envelope.error_message
        and (
            envelope.error_message == "Malformed response payload"
            or envelope.error_message.startswith("Invalid payload type")
        )
    )

    if is_candidate_for_shim:
        # Check if it's a raw list of rows (legacy format)
        import json

        raw_list = None
        if isinstance(payload, list):
            raw_list = payload
        elif isinstance(payload, str):
            try:
                parsed = json.loads(payload)
                if isinstance(parsed, list):
                    raw_list = parsed
            except json.JSONDecodeError:
                pass

        if raw_list is not None and get_env_bool("AGENT_ENABLE_LEGACY_TOOL_SHIM", False):
            # Shim: wrap list in envelope
            if telemetry.get_current_span():
                telemetry.get_current_span().add_event(
                    "tool.response_legacy_shape", {"type": "list"}
                )

            return ExecuteSQLQueryResponseEnvelope(
                rows=raw_list,
                metadata=ExecuteSQLQueryMetadata(
                    rows_returned=len(raw_list),
                    is_truncated=False,
                ),
            )

    return envelope


def _schema_drift_hint(
    error_text: str,
    provider: str,
    sql: Optional[str] = None,
    raw_schema_context: Optional[list[dict]] = None,
) -> tuple[bool, list[str], Optional[DriftDetectionMethod]]:
    from agent.utils.drift_detection import detect_schema_drift

    identifiers, method = detect_schema_drift(
        sql or "", error_text, provider, raw_schema_context or []
    )
    return (len(identifiers) > 0, identifiers, method)


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
            replay_bundle = state.get("replay_bundle")

            # Structured Concurrency for Prefetch
            async with PrefetchManager(max_concurrency=prefetch_max_concurrency) as prefetcher:
                if prefetch_enabled and page_token:
                    prefetched_cache_key = build_prefetch_cache_key(
                        sql_query=rewritten_sql,
                        tenant_id=tenant_id,
                        page_token=str(page_token),
                        page_size=execute_payload.get("page_size"),
                        schema_snapshot_id=state.get("schema_snapshot_id"),
                        seed=seed,
                        completeness_hint=completeness_hint,
                        scope_id=None,
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
                        replayed_output = lookup_replay_tool_output(
                            replay_bundle, "execute_sql_query", execute_payload
                        )
                        if replayed_output:
                            result = replayed_output
                            prefetch_reason = "replayed"
                        else:
                            result = await executor_tool.ainvoke(execute_payload)
                            prefetch_reason = "cache_miss"
                else:
                    replayed_output = lookup_replay_tool_output(
                        replay_bundle, "execute_sql_query", execute_payload
                    )
                    if replayed_output:
                        result = replayed_output
                    else:
                        result = await executor_tool.ainvoke(execute_payload)
                first_page_latency_seconds = max(0.0, time.monotonic() - first_page_started_at)

                # --- Typed Parsing ---
                envelope = _parse_tool_response_with_shim(result)

                # Map envelope to local vars
                result_is_truncated = envelope.metadata.is_truncated
                result_row_limit = envelope.metadata.row_limit
                result_rows_returned = envelope.metadata.rows_returned
                result_columns = envelope.columns
                result_next_page_token = envelope.metadata.next_page_token
                result_page_size = None
                result_partial_reason = envelope.metadata.partial_reason

                result_capability_required = envelope.metadata.capability_required
                result_capability_supported = envelope.metadata.capability_supported
                result_fallback_policy = envelope.metadata.fallback_policy
                result_fallback_applied = envelope.metadata.fallback_applied
                result_fallback_mode = envelope.metadata.fallback_mode

                result_cap_detected = envelope.metadata.cap_detected
                result_cap_mitigation_applied = envelope.metadata.cap_mitigation_applied
                result_cap_mitigation_mode = envelope.metadata.cap_mitigation_mode

                result_auto_paginated = False
                result_pages_fetched = 1
                result_auto_pagination_stopped_reason = PaginationStopReason.DISABLED.value
                result_prefetch_enabled = prefetch_enabled
                result_prefetch_scheduled = False
                result_prefetch_reason = prefetch_reason

                def _maybe_add_schema_drift(error_msg: str) -> dict:
                    if not get_env_bool("AGENT_SCHEMA_DRIFT_HINTS", True):
                        return {}
                    provider = get_env_str(
                        "QUERY_TARGET_BACKEND", get_env_str("QUERY_TARGET_PROVIDER", "postgres")
                    )
                    sql = state.get("current_sql")
                    raw_schema_context = state.get("raw_schema_context")
                    suspected, identifiers, method = _schema_drift_hint(
                        error_msg, provider, sql, raw_schema_context
                    )
                    if not suspected:
                        return {}
                    auto_refresh = get_env_bool("AGENT_SCHEMA_DRIFT_AUTO_REFRESH", False)
                    span.set_attribute("schema.drift.suspected", True)
                    span.set_attribute("schema.drift.missing_identifiers_count", len(identifiers))
                    span.set_attribute("schema.drift.auto_refresh_enabled", auto_refresh)
                    if method:
                        span.set_attribute("schema.drift.detection_method", method.value)
                    return {
                        "schema_drift_suspected": True,
                        "missing_identifiers": identifiers,
                        "schema_snapshot_id": state.get("schema_snapshot_id"),
                        "schema_drift_auto_refresh": auto_refresh,
                    }

                if envelope.is_error():
                    error_obj = envelope.error
                    error_msg = (
                        error_obj.message
                        if error_obj
                        else (envelope.error_message or "Unknown error")
                    )
                    error_category = error_obj.category if error_obj else None
                    error_metadata = error_obj.to_dict() if error_obj else None

                    if error_msg == "Malformed response payload":
                        span.set_attribute("tool.response_shape", "malformed")
                        trace_id = telemetry.get_current_trace_id()
                        error_msg = (
                            f"Tool response malformed. Trace ID: {trace_id or 'unavailable'}."
                        )
                        error_category = "tool_response_malformed"
                    else:
                        span.set_attribute("tool.response_shape", "error")

                    retry_after_seconds = error_obj.retry_after_seconds if error_obj else None

                    if error_metadata:
                        req_cap = error_metadata.get("required_capability")
                        if req_cap:
                            result_capability_required = req_cap
                            result_capability_supported = error_metadata.get("capability_supported")
                            result_fallback_policy = error_metadata.get("fallback_policy")
                            result_fallback_applied = error_metadata.get("fallback_applied")
                            result_fallback_mode = error_metadata.get("fallback_mode")
                            provider = error_metadata.get("provider")
                            if provider:
                                span.set_attribute("error.provider", provider)

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
                        if result_capability_required:
                            error_msg = (
                                "This backend does not support "
                                f"{result_capability_required} for this request."
                            )
                        else:
                            error_msg = (
                                "This backend does not support a required capability "
                                "for this request."
                            )
                        if result_capability_required:
                            span.set_attribute(
                                "error.required_capability", result_capability_required
                            )
                        if result_capability_supported is not None:
                            span.set_attribute(
                                "error.capability_supported", bool(result_capability_supported)
                            )
                        if result_fallback_policy:
                            span.set_attribute("error.fallback_policy", str(result_fallback_policy))
                        if result_fallback_applied is not None:
                            span.set_attribute(
                                "error.fallback_applied", bool(result_fallback_applied)
                            )
                        if result_fallback_mode:
                            span.set_attribute("error.fallback_mode", str(result_fallback_mode))

                    drift_hint = _maybe_add_schema_drift(error_msg)
                    return {
                        "error": error_msg,
                        "query_result": None,
                        "error_category": error_category,
                        "retry_after_seconds": retry_after_seconds,
                        "error_metadata": error_metadata,
                        **drift_hint,
                    }

                # Success path
                span.set_attribute("tool.response_shape", "enveloped")
                query_result = envelope.rows

                auto_pagination_enabled, auto_max_pages, auto_max_rows = _auto_pagination_config()
                if auto_pagination_enabled:
                    result_auto_pagination_stopped_reason = PaginationStopReason.NO_NEXT_PAGE.value
                if auto_pagination_enabled and len(query_result) > auto_max_rows:
                    query_result = query_result[:auto_max_rows]
                    result_is_truncated = True
                    if not result_partial_reason:
                        result_partial_reason = "LIMITED"
                    result_rows_returned = len(query_result)
                    result_auto_pagination_stopped_reason = PaginationStopReason.MAX_ROWS.value
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
                    seen_tokens = {str(result_next_page_token)}

                    while next_page_token:
                        if result_pages_fetched >= auto_max_pages:
                            result_auto_pagination_stopped_reason = (
                                PaginationStopReason.MAX_PAGES.value
                            )
                            break
                        if len(aggregated_rows) >= auto_max_rows:
                            result_auto_pagination_stopped_reason = (
                                PaginationStopReason.MAX_ROWS.value
                            )
                            break

                        page_timeout = None
                        if deadline_ts is not None:
                            page_timeout = max(0.0, deadline_ts - time.monotonic())
                            if page_timeout <= 0.5:  # Grace period
                                result_auto_pagination_stopped_reason = (
                                    PaginationStopReason.BUDGET_EXHAUSTED.value
                                )
                                break

                        page_payload = {
                            **execute_payload,
                            "page_token": next_page_token,
                            "page_size": execute_payload.get("page_size") or result_page_size,
                            "timeout_seconds": page_timeout,
                        }

                        try:
                            replayed_page = lookup_replay_tool_output(
                                replay_bundle, "execute_sql_query", page_payload
                            )
                            if replayed_page:
                                page_result = replayed_page
                            else:
                                page_result = await executor_tool.ainvoke(page_payload)

                            # Typed Parsing for Pages
                            page_envelope = _parse_tool_response_with_shim(page_result)

                            span.add_event(
                                "pagination.auto_page_fetch",
                                {
                                    "page_number": result_pages_fetched + 1,
                                    "has_next_page_token": bool(next_page_token),
                                    "max_pages": auto_max_pages,
                                    "max_rows": auto_max_rows,
                                },
                            )

                            if page_envelope.is_error():
                                result_auto_pagination_stopped_reason = (
                                    PaginationStopReason.FETCH_ERROR.value
                                )
                                break

                            page_rows = page_envelope.rows
                            page_meta = page_envelope.metadata

                            if not page_rows and page_meta.next_page_token:
                                if (
                                    result_auto_pagination_stopped_reason
                                    == PaginationStopReason.EMPTY_PAGE_WITH_TOKEN.value
                                ):
                                    result_auto_pagination_stopped_reason = (
                                        PaginationStopReason.PATHOLOGICAL_EMPTY_PAGES.value
                                    )
                                    break
                                result_auto_pagination_stopped_reason = (
                                    PaginationStopReason.EMPTY_PAGE_WITH_TOKEN.value
                                )

                            aggregated_rows.extend(page_rows)
                            result_pages_fetched += 1

                            next_page_token = page_meta.next_page_token

                            if next_page_token:
                                token_str = str(next_page_token)
                                if token_str in seen_tokens:
                                    result_auto_pagination_stopped_reason = (
                                        PaginationStopReason.TOKEN_REPEAT.value
                                    )
                                    next_page_token = None
                                    break
                                seen_tokens.add(token_str)

                            result_next_page_token = next_page_token
                            result_is_truncated = bool(result_is_truncated) or bool(
                                page_meta.is_truncated
                            )

                            if page_meta.row_limit is not None:
                                result_row_limit = page_meta.row_limit

                            if page_meta.rows_returned is not None:
                                result_rows_returned = page_meta.rows_returned

                            if page_meta.partial_reason:
                                result_partial_reason = page_meta.partial_reason

                            if page_meta.capability_required is not None:
                                result_capability_required = page_meta.capability_required

                            if page_meta.capability_supported is not None:
                                result_capability_supported = page_meta.capability_supported

                            if page_meta.fallback_policy is not None:
                                result_fallback_policy = page_meta.fallback_policy

                            if page_meta.fallback_applied is not None:
                                result_fallback_applied = page_meta.fallback_applied

                            if page_meta.fallback_mode:
                                result_fallback_mode = page_meta.fallback_mode

                            if page_meta.cap_detected is not None:
                                result_cap_detected = bool(result_cap_detected) or bool(
                                    page_meta.cap_detected
                                )

                            if page_meta.cap_mitigation_applied is not None:
                                result_cap_mitigation_applied = bool(
                                    result_cap_mitigation_applied
                                ) or bool(page_meta.cap_mitigation_applied)

                            if page_meta.cap_mitigation_mode:
                                result_cap_mitigation_mode = page_meta.cap_mitigation_mode

                            if len(aggregated_rows) >= auto_max_rows:
                                aggregated_rows = aggregated_rows[:auto_max_rows]
                                result_is_truncated = True
                                if not result_partial_reason:
                                    result_partial_reason = "LIMITED"
                                result_auto_pagination_stopped_reason = (
                                    PaginationStopReason.MAX_ROWS.value
                                )
                                break
                            if not next_page_token:
                                result_auto_pagination_stopped_reason = (
                                    PaginationStopReason.NO_NEXT_PAGE.value
                                )
                        except Exception as e:
                            logger.warning(f"Auto-pagination page fetch failed: {e}")
                            result_auto_pagination_stopped_reason = (
                                PaginationStopReason.FETCH_EXCEPTION.value
                            )
                            break

                    query_result = aggregated_rows
                    result_rows_returned = len(query_result)
                    result_auto_paginated = result_pages_fetched > 1
                elif (
                    auto_pagination_enabled
                    and result_capability_required == "pagination"
                    and result_capability_supported is False
                ):
                    result_auto_pagination_stopped_reason = (
                        PaginationStopReason.UNSUPPORTED_CAPABILITY.value
                    )

                if prefetch_enabled:
                    if result_prefetch_reason == "cache_hit":
                        pass
                    elif result_auto_paginated:
                        result_prefetch_reason = (
                            PrefetchSuppressionReason.AUTO_PAGINATION_ACTIVE.value
                        )
                    elif auto_pagination_enabled:
                        result_prefetch_reason = (
                            PrefetchSuppressionReason.AUTO_PAGINATION_ENABLED.value
                        )
                    elif not result_next_page_token:
                        result_prefetch_reason = PrefetchSuppressionReason.NO_NEXT_PAGE.value
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
                            result_prefetch_reason = PrefetchSuppressionReason.NOT_CHEAP.value
                        else:
                            prefetch_timeout = None
                            if deadline_ts is not None:
                                prefetch_timeout = max(
                                    0.0, min(2.0, deadline_ts - time.monotonic())
                                )
                                if prefetch_timeout <= 0.5:
                                    result_prefetch_reason = (
                                        PrefetchSuppressionReason.LOW_BUDGET.value
                                    )
                            if result_prefetch_reason not in {
                                PrefetchSuppressionReason.LOW_BUDGET.value,
                                PrefetchSuppressionReason.NOT_CHEAP.value,
                            }:
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
                                    replayed_prefetch = lookup_replay_tool_output(
                                        replay_bundle, "execute_sql_query", prefetch_payload
                                    )
                                    if replayed_prefetch:
                                        prefetched_raw = replayed_prefetch
                                    else:
                                        prefetched_raw = await executor_tool.ainvoke(
                                            prefetch_payload
                                        )
                                    # Parse as envelope
                                    prefetched_env = _parse_tool_response_with_shim(prefetched_raw)
                                    if prefetched_env.is_error():
                                        return None
                                    return prefetched_env.model_dump(exclude_none=True)

                                result_prefetch_scheduled = prefetcher.schedule(
                                    prefetch_key,
                                    _fetch_prefetched_page,
                                )
                                if result_prefetch_scheduled:
                                    result_prefetch_reason = (
                                        PrefetchSuppressionReason.SCHEDULED.value
                                    )
                                else:
                                    result_prefetch_reason = (
                                        PrefetchSuppressionReason.ALREADY_CACHED_OR_INFLIGHT.value
                                    )

                # Emit event if prefetch was enabled but not scheduled (and not cache hit)
                if (
                    prefetch_enabled
                    and not result_prefetch_scheduled
                    and result_prefetch_reason != PrefetchSuppressionReason.CACHE_HIT.value
                ):
                    prefetch_summary = format_decision_summary(
                        action="prefetch",
                        decision="suppress",
                        reason_code=result_prefetch_reason,
                    )
                    span.add_event("system.decision", prefetch_summary.to_dict())
                elif prefetch_enabled and result_prefetch_scheduled:
                    prefetch_summary = format_decision_summary(
                        action="prefetch",
                        decision="proceed",
                        reason_code=result_prefetch_reason,
                    )
                    span.add_event("system.decision", prefetch_summary.to_dict())

                result_columns = envelope.columns
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
                    span.set_attribute(
                        "result.rows_returned", len(query_result) if query_result else 0
                    )
                span.set_attribute("result.columns_available", bool(result_columns))
                span.set_attribute("timeout.triggered", False)
                if result_partial_reason:
                    span.set_attribute("result.partial_reason", result_partial_reason)
                is_limited = bool(state.get("result_is_limited"))
                span.set_attribute("result.is_limited", is_limited)
                if result_capability_required:
                    span.set_attribute("capability.required", result_capability_required)
                if result_capability_supported is not None:
                    span.set_attribute("capability.supported", bool(result_capability_supported))
                if result_fallback_policy:
                    span.set_attribute("capability.fallback_policy", str(result_fallback_policy))
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
                    span.set_attribute(
                        "result.cap_mitigation_mode", str(result_cap_mitigation_mode)
                    )
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
                from_cache = state.get("from_cache", False)
                if not error and original_sql and tenant_id and not from_cache:
                    try:
                        cache_tool = next((t for t in tools if t.name == "update_cache"), None)
                        if cache_tool:
                            user_query = (
                                state["messages"][-1].content if state.get("messages") else ""
                            )
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
                    "result_capability_required": result_capability_required,
                    "result_capability_supported": result_capability_supported,
                    "result_fallback_policy": result_fallback_policy,
                    "result_fallback_applied": bool(result_fallback_applied),
                    "result_fallback_mode": result_fallback_mode,
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
                    "last_tool_output": result if isinstance(result, dict) else None,
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
