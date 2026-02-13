"""LangGraph workflow definition for Text 2 SQL agent with MLflow tracing."""

import asyncio
import inspect
import json
import logging
import re
import time
import uuid
from typing import Any, Optional

from langgraph.checkpoint.memory import MemorySaver
from langgraph.graph import END, StateGraph

from agent.nodes.cache_lookup import cache_lookup_node
from agent.nodes.clarify import clarify_node
from agent.nodes.correct import correct_sql_node
from agent.nodes.execute import validate_and_execute_node
from agent.nodes.generate import generate_sql_node
from agent.nodes.plan import plan_sql_node
from agent.nodes.retrieve import retrieve_context_node
from agent.nodes.router import router_node
from agent.nodes.synthesize import synthesize_insight_node
from agent.nodes.validate import validate_sql_node
from agent.nodes.visualize import visualize_query_node
from agent.runtime_metrics import (
    record_query_complexity_score,
    record_stage_latency_breakdown,
    record_truncation_event,
)
from agent.state import AgentState
from agent.state.decision_events import append_decision_event
from agent.state.decision_summary import (
    build_decision_summary,
    build_retry_correction_summary,
    build_run_decision_summary,
)
from agent.state.run_summary_store import get_run_summary_store
from agent.telemetry import SpanType, telemetry
from agent.utils.retry_after import compute_retry_delay
from agent.utils.schema_snapshot import (
    apply_pending_schema_snapshot_refresh,
    resolve_pinned_schema_snapshot_id,
)
from common.config.env import get_env_bool, get_env_float, get_env_int, get_env_str
from common.constants.reason_codes import RetryDecisionReason
from common.observability.metrics import agent_metrics
from common.policy.sql_policy import load_policy_snapshot
from common.utils.decisions import format_decision_summary

logger = logging.getLogger(__name__)
_TRACE_ID_RE = re.compile(r"^[0-9a-f]{32}$")


def _safe_env_int(name: str, default: int, minimum: int) -> int:
    try:
        parsed = get_env_int(name, default)
    except ValueError:
        logger.warning("Invalid %s; using default %s", name, default)
        return default
    if parsed is None:
        return default
    return max(minimum, int(parsed))


def _is_replay_mode(state: AgentState) -> bool:
    return bool(state.get("replay_mode"))


def _resolve_run_seed(*, replay_mode: bool) -> Optional[int]:
    """Resolve deterministic run seed, forcing one for replay mode."""
    if replay_mode:
        try:
            seed = get_env_int("AGENT_LLM_SEED", 0)
        except ValueError:
            return 0
        if seed is None:
            return 0
        return int(seed)

    try:
        seed = get_env_int("AGENT_LLM_SEED", None)
    except ValueError:
        return None
    if seed is None:
        return None
    return int(seed)


def run_telemetry_configure():
    """Configure telemetry at runtime to avoid import-time side effects."""
    # Configure Telemetry (OTEL only)
    telemetry.configure()


def with_telemetry_context(node_func):
    """Wrap a node function to restore telemetry context."""

    async def wrapped_node(state: AgentState):
        raw_ctx = state.get("telemetry_context")
        if raw_ctx:
            # Deserialize the context from the state
            ctx = telemetry.deserialize_context(raw_ctx)
            with telemetry.use_context(ctx):
                ret = node_func(state)
                if inspect.isawaitable(ret):
                    return await ret
                return ret
        ret = node_func(state)
        if inspect.isawaitable(ret):
            return await ret
        return ret

    wrapped_node.__name__ = node_func.__name__
    return wrapped_node


async def schema_refresh_node(state: AgentState) -> dict:
    """Node to handle automatic schema refresh on drift detection."""
    with telemetry.start_span(
        name="schema_refresh",
        span_type=SpanType.AGENT_NODE,
    ) as span:
        from agent.tools import get_mcp_tools
        from agent.utils.parsing import parse_tool_output, unwrap_envelope
        from agent.utils.schema_fingerprint import (
            fingerprint_schema_nodes,
            resolve_schema_snapshot_id,
        )

        refresh_count = state.get("schema_refresh_count", 0)
        span.set_attribute("schema.drift.auto_refresh_attempted", True)
        span.set_attribute("schema.drift.refresh_count", refresh_count + 1)
        prior_pinned_snapshot_id = resolve_pinned_schema_snapshot_id(state)
        span.set_attribute("schema.old_snapshot_id", prior_pinned_snapshot_id)

        # Invalidate cache for missing identifiers
        from common.config.env import get_env_str
        from dal.schema_cache import SCHEMA_CACHE

        provider = get_env_str("QUERY_TARGET_BACKEND", "postgres")
        missing = state.get("missing_identifiers") or []
        span.set_attribute("schema.drift.missing_identifiers", missing)

        for identifier in missing:
            # Try to parse schema.table
            parts = identifier.split(".")
            if len(parts) == 2:
                SCHEMA_CACHE.invalidate(provider=provider, schema=parts[0], table=parts[1])
            else:
                # Fallback: invalidate table in default schema
                SCHEMA_CACHE.invalidate(provider=provider, table=identifier)

        candidate_snapshot_id = state.get("pending_schema_snapshot_id")
        candidate_fingerprint = state.get("pending_schema_fingerprint")
        candidate_version_ts = state.get("pending_schema_version_ts")
        if not candidate_snapshot_id:
            active_query = state.get("active_query")
            if not active_query:
                messages = state.get("messages") or []
                if messages:
                    active_query = getattr(messages[-1], "content", None)

            if active_query:
                try:
                    tools = await get_mcp_tools()
                    subgraph_tool = next(
                        (
                            tool
                            for tool in tools
                            if getattr(tool, "name", None) == "get_semantic_subgraph"
                        ),
                        None,
                    )
                    if subgraph_tool is not None:
                        payload = {"query": active_query}
                        tenant_id = state.get("tenant_id")
                        if tenant_id is not None:
                            payload["tenant_id"] = tenant_id

                        raw_subgraph = await subgraph_tool.ainvoke(payload)
                        parsed = parse_tool_output(raw_subgraph)
                        if isinstance(parsed, list) and parsed:
                            parsed = parsed[0]
                        parsed = unwrap_envelope(parsed)
                        nodes = parsed.get("nodes", []) if isinstance(parsed, dict) else []
                        resolved_snapshot_id = resolve_schema_snapshot_id(nodes)
                        if resolved_snapshot_id and resolved_snapshot_id != "unknown":
                            candidate_snapshot_id = resolved_snapshot_id
                            candidate_fingerprint = (
                                fingerprint_schema_nodes(nodes) if nodes else None
                            )
                            candidate_version_ts = (
                                int(time.time()) if candidate_fingerprint else candidate_version_ts
                            )
                except Exception:
                    logger.warning(
                        "Schema refresh could not resolve updated snapshot id",
                        exc_info=True,
                    )

        refresh_state = apply_pending_schema_snapshot_refresh(
            state,
            candidate_snapshot_id=candidate_snapshot_id,
            candidate_fingerprint=candidate_fingerprint,
            candidate_version_ts=candidate_version_ts,
            reason="schema_refresh",
        )
        span.set_attribute("schema.new_snapshot_id", refresh_state["schema_snapshot_id"])
        span.set_attribute(
            "schema.refresh_applied",
            refresh_state["schema_snapshot_id"] != prior_pinned_snapshot_id,
        )

        return {
            "schema_refresh_count": refresh_count + 1,
            "error": None,  # Clear error to allow re-entry into the flow
            "schema_drift_suspected": False,
            "retry_count": state.get("retry_count", 0),  # Preserve retry count
            **refresh_state,
        }


def route_after_router(state: AgentState) -> str:
    """
    Conditional edge logic after router node.

    Routes to clarify if ambiguity detected, otherwise to plan.
    Note: Retrieve has already run, so router has schema_context.

    Args:
        state: Current agent state

    Returns:
        str: Next node name
    """
    if state.get("ambiguity_type"):
        return "clarify"
    return "plan"


def route_after_cache_lookup(state: AgentState) -> str:
    """
    Conditional edge logic after cache lookup.

    Routes based on cache status:
    - If hit and valid (from_cache=True): go to AST validation (then execute)
    - If miss or invalid (from_cache=False): go to retrieve (schema lookup)
    """
    if state.get("from_cache"):
        return "validate"
    return "retrieve"


def route_after_validation(state: AgentState) -> str:
    """
    Conditional edge logic after SQL validation.

    Routes based on AST validation result:
    - If validation passed: go to execute
    - If validation failed and budget available: go to correction
    - If validation failed and budget exhausted: go to synthesize

    Args:
        state: Current agent state

    Returns:
        str: Next node name
    """
    retry_count = state.get("retry_count", 0)
    max_retries = 0 if _is_replay_mode(state) else _safe_env_int("AGENT_MAX_RETRIES", 3, 0)
    error_category = str(state.get("error_category") or "").strip().lower()

    ast_result = state.get("ast_validation_result")
    is_invalid = (ast_result and not ast_result.get("is_valid")) or state.get("error")

    if error_category in {"budget_exceeded", "budget_exhausted"}:
        return "synthesize"

    if is_invalid:
        if retry_count < max_retries:
            return "correct"
        return "synthesize"

    return "execute"


def _retry_policy_mode() -> str:
    mode = (get_env_str("AGENT_RETRY_POLICY", "adaptive") or "adaptive").strip().lower()
    if mode == "adaptive":
        return "adaptive"
    return "static"


def _max_retry_attempts() -> int:
    try:
        configured = get_env_int("AGENT_MAX_RETRIES", 3)
    except ValueError:
        logger.warning("Invalid AGENT_MAX_RETRIES value; defaulting to 3")
        configured = 3
    if configured is None:
        configured = 3
    return max(1, int(configured))


def _effective_max_retry_attempts(state: AgentState) -> int:
    if _is_replay_mode(state):
        return 0
    return _max_retry_attempts()


def _adaptive_is_retryable(
    error_category: str | None, error_metadata: Optional[dict[str, Any]] = None
) -> bool:
    if isinstance(error_metadata, dict):
        classified_retryable = error_metadata.get("is_retryable")
        if isinstance(classified_retryable, bool):
            return classified_retryable

    non_retryable = {
        "unsupported_capability",
        "auth",
        "invalid_request",
        "tool_response_malformed",
        "budget_exhausted",
        "budget_exceeded",
    }
    if not error_category:
        return True
    return error_category not in non_retryable


def _set_retry_reason(
    state: AgentState,
    *,
    reason_code: RetryDecisionReason,
    will_retry: bool,
    retry_policy: Optional[str],
    retry_count: int,
    span: Optional[Any],
) -> None:
    """Persist stable retry reason metadata and emit structured telemetry attributes."""
    reason_value = reason_code.value
    state["retry_reason"] = reason_value

    existing_error_metadata = state.get("error_metadata")
    error_metadata = (
        existing_error_metadata.copy() if isinstance(existing_error_metadata, dict) else {}
    )
    error_metadata["retry_reason"] = reason_value
    error_metadata["retry_will_retry"] = bool(will_retry)
    if retry_policy:
        error_metadata["retry_policy"] = retry_policy
    state["error_metadata"] = error_metadata

    if span:
        span.set_attribute("retry.reason_code", reason_value)
        span.set_attribute("retry.will_retry", bool(will_retry))
        if retry_policy:
            span.set_attribute("retry.policy", retry_policy)
        span.add_event(
            "retry.decision",
            {
                "reason_code": reason_value,
                "will_retry": bool(will_retry),
                "policy": retry_policy or "unknown",
            },
        )

    metric_attributes = {
        "policy": retry_policy or "unknown",
        "reason": reason_value,
        "will_retry": bool(will_retry),
    }
    agent_metrics.add_counter(
        "agent.retry.decisions_total",
        attributes=metric_attributes,
        description="Count of retry decision outcomes",
    )
    agent_metrics.record_histogram(
        "agent.retry.attempt_number",
        value=float(retry_count),
        unit="attempt",
        description="Observed retry attempt index for retry decisions",
        attributes=metric_attributes,
    )


def route_after_execution(state: AgentState) -> str:
    """
    Conditional edge logic after SQL execution.

    Determines the next step based on execution result:
    - If error and retries < 3: go to correction
    - If error and retries >= 3: go to failure
    - If success: go to synthesis

    Args:
        state: Current agent state

    Returns:
        str: Next node name
    """
    if not state.get("error"):
        state["retry_reason"] = None
        return "visualize"  # Go to visualization (then synthesis)

    state["retry_reason"] = None
    error_category = state.get("error_category")
    if error_category == "unsupported_capability":
        span = telemetry.get_current_span()
        retry_policy = _retry_policy_mode()
        _set_retry_reason(
            state,
            reason_code=RetryDecisionReason.UNSUPPORTED_CAPABILITY,
            will_retry=False,
            retry_policy=retry_policy,
            retry_count=int(state.get("retry_count", 0) or 0),
            span=span,
        )
        if span:
            span.set_attribute("retry.stopped_due_to_capability", True)
            span.add_event(
                "retry.decision",
                {
                    "category": "unsupported_capability",
                    "is_retryable": False,
                    "reason_code": RetryDecisionReason.UNSUPPORTED_CAPABILITY.value,
                    "will_retry": False,
                },
            )
        append_decision_event(
            state,
            node="route_after_execution",
            decision="fail",
            reason=RetryDecisionReason.UNSUPPORTED_CAPABILITY.value,
            retry_count=int(state.get("retry_count", 0) or 0),
            error_category=error_category,
            span=span,
        )
        return "failed"

    # Guarded Automatic Schema Refresh
    if (
        state.get("schema_drift_suspected")
        and state.get("schema_drift_auto_refresh")
        and not _is_replay_mode(state)
    ):
        refresh_count = state.get("schema_refresh_count", 0)
        if refresh_count < 1:
            append_decision_event(
                state,
                node="route_after_execution",
                decision="refresh_schema",
                reason="schema_drift_auto_refresh",
                retry_count=int(state.get("retry_count", 0) or 0),
                error_category=error_category,
                span=telemetry.get_current_span(),
            )
            return "refresh_schema"

    deadline_ts = state.get("deadline_ts")
    remaining = None
    estimated_correction_budget = _estimate_correction_budget_seconds(state)
    retry_count = state.get("retry_count", 0)
    max_retries = _effective_max_retry_attempts(state)
    retry_policy = _retry_policy_mode()
    retry_after_seconds = state.get("retry_after_seconds")

    # Initialize or update retry_summary
    retry_summary = state.get("retry_summary") or {"attempts": [], "budget_exhausted": False}
    retry_summary["policy"] = retry_policy
    retry_summary["attempts"].append(
        {
            "retry_number": retry_count,
            "error_category": error_category,
            "timestamp": time.monotonic(),
        }
    )

    span = telemetry.get_current_span()
    if span:
        span.set_attribute("retry.policy", retry_policy)
        span.set_attribute("retry.max_retries", max_retries)
        span.set_attribute("retry.attempt_number", retry_count)

    if deadline_ts is not None:
        remaining = deadline_ts - time.monotonic()

    bounded_retry_after = 0.0
    required_budget = estimated_correction_budget
    retry_decision = {
        "category": error_category or "unknown",
        "policy": retry_policy,
        "retry_count": retry_count,
        "max_retries": max_retries,
        "remaining_budget": max(0.0, remaining) if remaining is not None else None,
    }

    if retry_policy == "adaptive":
        is_retryable = _adaptive_is_retryable(error_category, state.get("error_metadata"))
        retry_summary["is_retryable"] = is_retryable
        retry_decision["is_retryable"] = is_retryable
        if span:
            span.set_attribute("retry.is_retryable", is_retryable)
        if not is_retryable:
            retry_summary["stopped_non_retryable"] = True
            state["retry_summary"] = retry_summary
            retry_decision["reason_code"] = RetryDecisionReason.NON_RETRYABLE_CATEGORY.value
            retry_decision["will_retry"] = False
            _set_retry_reason(
                state,
                reason_code=RetryDecisionReason.NON_RETRYABLE_CATEGORY,
                will_retry=False,
                retry_policy=retry_policy,
                retry_count=retry_count,
                span=span,
            )

            decision_summary = format_decision_summary(
                action="retry",
                decision="stop",
                reason_code=RetryDecisionReason.NON_RETRYABLE_CATEGORY,
                category=error_category,
            )
            if span:
                span.add_event("system.decision", decision_summary.to_dict())
            append_decision_event(
                state,
                node="route_after_execution",
                decision="fail",
                reason=RetryDecisionReason.NON_RETRYABLE_CATEGORY.value,
                retry_count=retry_count,
                error_category=error_category,
                span=span,
            )
            return "failed"

        if retry_after_seconds is not None and float(retry_after_seconds) > 0:
            retry_decision["retry_after_raw"] = float(retry_after_seconds)

            jittered_delay = compute_retry_delay(
                float(retry_after_seconds),
                jitter_ratio=get_env_float("AGENT_THROTTLE_JITTER_RATIO", 0.2),
                max_delay=get_env_float("AGENT_MAX_THROTTLE_SLEEP_SECONDS", 2.0),
            )

            if remaining is None:
                bounded_retry_after = jittered_delay
                retry_decision["retry_after_applied"] = True
            else:
                bounded_retry_after = min(jittered_delay, max(0.0, remaining))
                retry_decision["retry_after_applied"] = bounded_retry_after > 0
                if bounded_retry_after < jittered_delay:
                    retry_decision["retry_after_capped"] = True

            if bounded_retry_after <= 0.0 and remaining is not None and remaining <= 0:
                retry_summary["budget_exhausted"] = True
                state["retry_summary"] = retry_summary
                state["error_category"] = "timeout"
                retry_decision["reason_code"] = (
                    RetryDecisionReason.BUDGET_EXHAUSTED_RETRY_AFTER.value
                )
                retry_decision["will_retry"] = False
                _set_retry_reason(
                    state,
                    reason_code=RetryDecisionReason.BUDGET_EXHAUSTED_RETRY_AFTER,
                    will_retry=False,
                    retry_policy=retry_policy,
                    retry_count=retry_count,
                    span=span,
                )

                decision_summary = format_decision_summary(
                    action="retry",
                    decision="stop",
                    reason_code=RetryDecisionReason.BUDGET_EXHAUSTED_RETRY_AFTER,
                    remaining_budget=remaining,
                )
                if span:
                    span.add_event("system.decision", decision_summary.to_dict())
                append_decision_event(
                    state,
                    node="route_after_execution",
                    decision="stop_budget",
                    reason=RetryDecisionReason.BUDGET_EXHAUSTED_RETRY_AFTER.value,
                    retry_count=retry_count,
                    error_category=state.get("error_category"),
                    retry_after_seconds=bounded_retry_after,
                    span=span,
                )
                return "failed"
            state["retry_after_seconds"] = bounded_retry_after
            retry_summary["retry_after_seconds"] = bounded_retry_after
            required_budget += bounded_retry_after
            if span:
                span.set_attribute("retry.retry_after_seconds", bounded_retry_after)
        else:
            state["retry_after_seconds"] = None
    else:
        state["retry_after_seconds"] = None
        retry_decision["is_retryable"] = True  # Static policy treats all as retryable until max

    if deadline_ts is not None:
        retry_decision["required_budget"] = required_budget
        if span:
            span.set_attribute("retry.remaining_budget_seconds", max(0.0, remaining))
            span.set_attribute("retry.estimated_correction_budget", estimated_correction_budget)
            span.set_attribute("retry.budget.estimated_seconds", required_budget)
            span.set_attribute(
                "retry.budget.ema_latency_seconds", state.get("ema_llm_latency_seconds")
            )

        if remaining < required_budget:
            if span:
                span.set_attribute("retry.stopped_due_to_budget", True)
            retry_summary["budget_exhausted"] = True
            state["retry_summary"] = retry_summary
            state["error"] = (
                f"Retry budget exhausted after {retry_count} attempts; remaining time "
                f"{max(0.0, remaining):.2f}s is below estimated required "
                f"{required_budget:.2f}s."
            )
            state["error_category"] = "timeout"
            retry_decision["reason_code"] = RetryDecisionReason.INSUFFICIENT_BUDGET.value
            retry_decision["will_retry"] = False
            _set_retry_reason(
                state,
                reason_code=RetryDecisionReason.INSUFFICIENT_BUDGET,
                will_retry=False,
                retry_policy=retry_policy,
                retry_count=retry_count,
                span=span,
            )

            decision_summary = format_decision_summary(
                action="retry",
                decision="stop",
                reason_code=RetryDecisionReason.INSUFFICIENT_BUDGET,
                remaining_budget=remaining,
                required_budget=required_budget,
            )
            if span:
                span.add_event("system.decision", decision_summary.to_dict())
            append_decision_event(
                state,
                node="route_after_execution",
                decision="stop_budget",
                reason=RetryDecisionReason.INSUFFICIENT_BUDGET.value,
                retry_count=retry_count,
                error_category=state.get("error_category"),
                retry_after_seconds=bounded_retry_after if bounded_retry_after > 0 else None,
                span=span,
            )
            return "failed"

    if span:
        span.set_attribute("retry.stopped_due_to_budget", False)

    if deadline_ts is not None and time.monotonic() >= deadline_ts:
        retry_summary["budget_exhausted"] = True
        state["retry_summary"] = retry_summary
        retry_decision["reason_code"] = RetryDecisionReason.DEADLINE_EXCEEDED.value
        retry_decision["will_retry"] = False
        _set_retry_reason(
            state,
            reason_code=RetryDecisionReason.DEADLINE_EXCEEDED,
            will_retry=False,
            retry_policy=retry_policy,
            retry_count=retry_count,
            span=span,
        )

        decision_summary = format_decision_summary(
            action="retry",
            decision="stop",
            reason_code=RetryDecisionReason.DEADLINE_EXCEEDED,
        )
        if span:
            span.add_event("system.decision", decision_summary.to_dict())
        append_decision_event(
            state,
            node="route_after_execution",
            decision="stop_budget",
            reason=RetryDecisionReason.DEADLINE_EXCEEDED.value,
            retry_count=retry_count,
            error_category=state.get("error_category"),
            retry_after_seconds=bounded_retry_after if bounded_retry_after > 0 else None,
            span=span,
        )
        return "failed"

    if retry_count >= max_retries:
        retry_summary["max_retries_reached"] = True
        state["retry_summary"] = retry_summary
        retry_decision["reason_code"] = RetryDecisionReason.MAX_RETRIES_REACHED.value
        retry_decision["will_retry"] = False
        _set_retry_reason(
            state,
            reason_code=RetryDecisionReason.MAX_RETRIES_REACHED,
            will_retry=False,
            retry_policy=retry_policy,
            retry_count=retry_count,
            span=span,
        )

        decision_summary = format_decision_summary(
            action="retry",
            decision="stop",
            reason_code=RetryDecisionReason.MAX_RETRIES_REACHED,
            retry_count=retry_count,
        )
        if span:
            span.add_event("system.decision", decision_summary.to_dict())
        append_decision_event(
            state,
            node="route_after_execution",
            decision="fail",
            reason=RetryDecisionReason.MAX_RETRIES_REACHED.value,
            retry_count=retry_count,
            error_category=error_category,
            retry_after_seconds=bounded_retry_after if bounded_retry_after > 0 else None,
            span=span,
        )
        return "failed"

    state["retry_summary"] = retry_summary

    # Check if throttled
    reason_code = RetryDecisionReason.PROCEED_TO_CORRECTION
    if state.get("retry_after_seconds") and float(state["retry_after_seconds"]) > 0:
        reason_code = RetryDecisionReason.THROTTLE_RETRY

    retry_decision["reason_code"] = reason_code.value
    retry_decision["will_retry"] = True
    agent_metrics.add_counter(
        "agent.retry.decisions_total",
        attributes={
            "policy": retry_policy,
            "reason": reason_code.value,
            "will_retry": True,
        },
        description="Count of retry decision outcomes",
    )
    agent_metrics.record_histogram(
        "agent.retry.attempt_number",
        value=float(retry_count),
        unit="attempt",
        description="Observed retry attempt index for retry decisions",
        attributes={
            "policy": retry_policy,
            "reason": reason_code.value,
            "will_retry": True,
        },
    )

    decision_summary = format_decision_summary(
        action="retry",
        decision="proceed",
        reason_code=reason_code,
        attempt=retry_count + 1,
    )
    if span:
        span.add_event("system.decision", decision_summary.to_dict())
    append_decision_event(
        state,
        node="route_after_execution",
        decision="retry",
        reason=reason_code.value,
        retry_count=retry_count,
        error_category=error_category,
        retry_after_seconds=state.get("retry_after_seconds"),
        span=span,
    )
    return "correct"  # Go to self-correction


def _estimate_correction_budget_seconds(state: AgentState) -> float:
    """Estimate time needed for another correction attempt."""
    min_budget = get_env_float("AGENT_MIN_RETRY_BUDGET_SECONDS", 3.0) or 0.0
    # Fixed overhead captures prompt assembly + orchestration costs.
    overhead_seconds = 0.5
    ema_latency = state.get("ema_llm_latency_seconds")
    observed = state.get("latency_correct_seconds") or state.get("latency_generate_seconds")
    if ema_latency is None and observed is not None:
        ema_latency = observed
    if ema_latency is None:
        ema_latency = min_budget or 3.0
    estimated = float(ema_latency) + overhead_seconds
    if min_budget:
        estimated = max(estimated, float(min_budget))
    return estimated


def create_workflow() -> StateGraph:
    """
    Create and configure the LangGraph workflow.

    Flow (schema-aware clarification):
    cache_lookup → [validate → execute]
    OR [retrieve → router → plan → generate → validate → execute]

    The key insight: explicit cache lookup node acts as entry point to optimize latency.

    Returns:
        StateGraph: Configured workflow graph (not compiled)
    """
    workflow = StateGraph(AgentState)

    # Add all nodes with telemetry context wrapping
    workflow.add_node("cache_lookup", with_telemetry_context(cache_lookup_node))
    workflow.add_node("router", with_telemetry_context(router_node))
    workflow.add_node("clarify", with_telemetry_context(clarify_node))
    workflow.add_node("retrieve", with_telemetry_context(retrieve_context_node))
    workflow.add_node("plan", with_telemetry_context(plan_sql_node))
    workflow.add_node("generate", with_telemetry_context(generate_sql_node))
    workflow.add_node("validate", with_telemetry_context(validate_sql_node))
    workflow.add_node("execute", with_telemetry_context(validate_and_execute_node))
    workflow.add_node("refresh_schema", with_telemetry_context(schema_refresh_node))
    workflow.add_node("correct", with_telemetry_context(correct_sql_node))
    workflow.add_node("visualize", with_telemetry_context(visualize_query_node))
    workflow.add_node("synthesize", with_telemetry_context(synthesize_insight_node))

    # Set entry point - Cache Lookup first
    workflow.set_entry_point("cache_lookup")

    # Routing from Cache Lookup
    workflow.add_conditional_edges(
        "cache_lookup",
        route_after_cache_lookup,
        {
            "validate": "validate",
            "retrieve": "retrieve",
        },
    )

    # Retrieve feeds into router (router now has schema context)
    workflow.add_edge("retrieve", "router")

    # Router conditional edges (schema-aware ambiguity detection)
    workflow.add_conditional_edges(
        "router",
        route_after_router,
        {
            "clarify": "clarify",
            "plan": "plan",
        },
    )

    # Clarify loops back to router (to re-evaluate with clarification)
    # Note: No need to re-retrieve since schema hasn't changed
    workflow.add_edge("clarify", "router")

    # Main flow edges
    workflow.add_edge("plan", "generate")
    workflow.add_edge("generate", "validate")

    # Validation conditional edges
    workflow.add_conditional_edges(
        "validate",
        route_after_validation,
        {
            "execute": "execute",
            "correct": "correct",
            "synthesize": "synthesize",
        },
    )

    # Execution conditional edges (self-correction loop)
    workflow.add_conditional_edges(
        "execute",
        route_after_execution,
        {
            "correct": "correct",
            "visualize": "visualize",
            "refresh_schema": "refresh_schema",
            "failed": "synthesize",
        },
    )

    # Refresh schema loops back to retrieve to get fresh DDLs
    workflow.add_edge("refresh_schema", "retrieve")

    # Visualization feeds into synthesis
    workflow.add_edge("visualize", "synthesize")

    # Correction loops back to validate (to re-check corrected SQL)
    workflow.add_edge("correct", "validate")

    # Final edge
    workflow.add_edge("synthesize", END)

    return workflow


# Create checkpointer for interrupt support
memory = MemorySaver()

# Compile workflow with checkpointer
app = create_workflow().compile(checkpointer=memory)


# Wrapper function with MLflow tracing
async def run_agent_with_tracing(
    question: str,
    tenant_id: int,
    session_id: str = None,
    user_id: str = None,
    thread_id: str = None,
    schema_snapshot_id: str = None,
    timeout_seconds: float = None,
    deadline_ts: float = None,
    page_token: str = None,
    page_size: int = None,
    interactive_session: bool = False,
    replay_mode: bool = False,
    replay_bundle: Optional[dict[str, Any]] = None,
) -> dict:
    """Run agent workflow with tracing and context propagation."""
    from langchain_core.messages import HumanMessage

    from common.sanitization import sanitize_text

    if tenant_id is None:
        raise ValueError("tenant_id is required")

    from common.config.sanity import validate_runtime_configuration

    validate_runtime_configuration()

    # 0. Centralized Ingress Sanitization
    raw_question = question
    res = sanitize_text(question)
    # We use the sanitized version for all downstream processing
    # If sanitization fails completely (e.g. empty after trim),
    # we use an empty string which will trigger failure/clarification naturally.
    question = res.sanitized or ""

    # Ensure telemetry is configured at runtime
    run_telemetry_configure()

    # Generate thread_id if not provided (required for checkpointer and telemetry)
    if thread_id is None:
        thread_id = session_id or str(uuid.uuid4())

    # Generate stable run_id for this execution
    run_id = str(uuid.uuid4())

    # Prepare base metadata for all spans
    base_metadata = {
        "tenant_id": str(tenant_id),
        "run_id": run_id,
        "environment": get_env_str("ENVIRONMENT", "development"),
        "deployment": get_env_str("DEPLOYMENT", "development"),
        "version": "2.0.0",
        "thread_id": thread_id,
    }
    if replay_mode:
        base_metadata["replay_mode"] = "deterministic"
    if replay_bundle:
        base_metadata["replay_mode"] = "active"

    if session_id:
        base_metadata["telemetry.session_id"] = session_id
    if user_id:
        base_metadata["telemetry.user_id"] = user_id

    with telemetry.start_span("agent_workflow", span_type=SpanType.CHAIN, attributes=base_metadata):
        # Make metadata sticky for all child spans
        telemetry.update_current_trace(base_metadata)

        # Capture context and serialize it for state persistence
        telemetry_context = telemetry.capture_context()
        serialized_ctx = telemetry.serialize_context(telemetry_context)

        # Prepare initial state
        if deadline_ts is None and timeout_seconds:
            deadline_ts = time.monotonic() + timeout_seconds

        policy_snapshot = load_policy_snapshot()

        inputs = {
            "messages": [HumanMessage(content=question)],
            "run_id": run_id,
            "policy_snapshot": policy_snapshot,
            "schema_context": "",
            "current_sql": None,
            "query_result": None,
            "error": None,
            "retry_after_seconds": None,
            "retry_count": 0,
            "schema_refresh_count": 0,
            # Reset state fields that shouldn't persist across turns
            "active_query": None,
            "procedural_plan": None,
            "rejected_cache_context": None,
            "clause_map": None,
            "tenant_id": tenant_id,
            "from_cache": False,
            "telemetry_context": serialized_ctx,
            "raw_user_input": raw_question,
            "schema_snapshot_id": schema_snapshot_id,
            "pinned_schema_snapshot_id": schema_snapshot_id,
            "pending_schema_snapshot_id": None,
            "pending_schema_fingerprint": None,
            "pending_schema_version_ts": None,
            "schema_snapshot_transition": None,
            "schema_snapshot_refresh_applied": 0,
            "schema_fingerprint": None,
            "schema_version_ts": None,
            "deadline_ts": deadline_ts,
            "timeout_seconds": timeout_seconds,
            "page_token": page_token,
            "page_size": page_size,
            "seed": _resolve_run_seed(replay_mode=bool(replay_mode)),
            "interactive_session": interactive_session,
            "replay_mode": bool(replay_mode),
            "replay_bundle": replay_bundle,
            "token_budget": {
                "max_tokens": get_env_int("AGENT_TOKEN_BUDGET", 50000),
                "consumed_tokens": 0,
            },
            "llm_prompt_bytes_used": 0,
            "llm_budget_exceeded": False,
            "error_signatures": [],
            "decision_events": [],
            "decision_events_truncated": False,
            "decision_events_dropped": 0,
        }

        # Config with thread_id for checkpointer
        config = {"configurable": {"thread_id": thread_id}}

        # Execute workflow within MCP context to ensure connections are closed
        from agent.tools import mcp_tools_context, unpack_mcp_result
        from agent.utils.llm_run_budget import current_budget_state, llm_run_budget_context
        from agent.utils.retry import retry_with_backoff

        # Interaction persistence mode (default: best_effort)
        persistence_mode = (get_env_str("AGENT_INTERACTION_PERSISTENCE_MODE", "") or "").strip()
        if not persistence_mode:
            legacy_fail_open = get_env_bool("PERSISTENCE_FAIL_OPEN", None)
            if legacy_fail_open is True:
                persistence_mode = "best_effort"
            elif legacy_fail_open is False:
                persistence_mode = "strict"
            else:
                persistence_mode = "best_effort"
        if persistence_mode not in {"best_effort", "strict"}:
            logger.warning(
                "Invalid AGENT_INTERACTION_PERSISTENCE_MODE: %s; defaulting to best_effort",
                persistence_mode,
            )
            persistence_mode = "best_effort"

        base_llm_budget = _safe_env_int("AGENT_TOKEN_BUDGET", 50000, 0)
        llm_budget_limit = _safe_env_int("AGENT_LLM_TOKEN_BUDGET", base_llm_budget, 0)
        llm_calls_total = 0
        llm_token_total = 0

        with llm_run_budget_context(llm_budget_limit):
            async with mcp_tools_context() as tools:
                schema_snapshot_id = inputs.get("schema_snapshot_id")
                if not schema_snapshot_id:
                    snapshot_mode = (
                        get_env_str("SCHEMA_SNAPSHOT_MODE", "fingerprint").strip().lower()
                    )
                    if snapshot_mode == "static":
                        schema_snapshot_id = "v1.0"
                    elif snapshot_mode == "fingerprint":
                        from agent.utils.schema_cache import get_or_refresh_schema_snapshot_id

                        subgraph_tool = next(
                            (t for t in tools if t.name == "get_semantic_subgraph"), None
                        )
                        if subgraph_tool:

                            async def _refresh_snapshot_id() -> Optional[str]:
                                try:
                                    payload = {"query": question}
                                    if tenant_id is not None:
                                        payload["tenant_id"] = tenant_id
                                    raw_subgraph = await subgraph_tool.ainvoke(payload)
                                    from agent.utils.parsing import parse_tool_output
                                    from agent.utils.schema_fingerprint import (
                                        resolve_schema_snapshot_id,
                                    )

                                    parsed = parse_tool_output(raw_subgraph)
                                    if isinstance(parsed, list) and parsed:
                                        parsed = parsed[0]
                                    nodes = (
                                        parsed.get("nodes", []) if isinstance(parsed, dict) else []
                                    )
                                    return resolve_schema_snapshot_id(nodes)
                                except Exception:
                                    logger.warning(
                                        "Failed to compute schema snapshot id", exc_info=True
                                    )
                                    return None

                            schema_snapshot_id = await get_or_refresh_schema_snapshot_id(
                                tenant_id, _refresh_snapshot_id
                            )
                    schema_snapshot_id = schema_snapshot_id or "unknown"
                    inputs["schema_snapshot_id"] = schema_snapshot_id
                inputs["pinned_schema_snapshot_id"] = schema_snapshot_id
                telemetry.update_current_trace({"schema_snapshot_id": schema_snapshot_id})

                # 1. Start Interaction Logging (Pre-execution) with retry
                interaction_id = None
                interaction_persisted = False
                create_tool = next((t for t in tools if t.name == "create_interaction"), None)
                if create_tool:
                    try:

                        async def _create_interaction():
                            # Use canonical OTEL trace_id if available, fallback to thread_id
                            # This ensures the ID stored in DB matches the trace in Tempo/Grafana
                            otel_trace_id = telemetry.get_current_trace_id()
                            final_trace_id = (
                                otel_trace_id
                                if otel_trace_id and _TRACE_ID_RE.fullmatch(otel_trace_id)
                                else None
                            )

                            return await create_tool.ainvoke(
                                {
                                    "conversation_id": session_id or thread_id,
                                    "schema_snapshot_id": schema_snapshot_id or "unknown",
                                    "user_nlq_text": question,
                                    "tenant_id": tenant_id,
                                    "model_version": get_env_str("LLM_MODEL", "gpt-4o"),
                                    "prompt_version": "v1.0",
                                    "trace_id": final_trace_id,
                                },
                                config=config,
                            )

                        if telemetry.get_current_span():
                            telemetry.get_current_span().add_event("persistence.create.start")

                        persistence_timeout_ms = get_env_int(
                            "AGENT_INTERACTION_PERSISTENCE_TIMEOUT_MS", 500
                        )
                        persistence_fail_open = get_env_bool(
                            "AGENT_INTERACTION_PERSISTENCE_FAIL_OPEN", True
                        )

                        try:
                            raw_interaction_id = await asyncio.wait_for(
                                retry_with_backoff(
                                    _create_interaction,
                                    "create_interaction",
                                    extra_context={"trace_id": thread_id},
                                ),
                                timeout=persistence_timeout_ms / 1000.0,
                            )
                        except asyncio.TimeoutError:
                            logger.warning(
                                "Interaction creation timed out after %dms", persistence_timeout_ms
                            )
                            telemetry.add_event(
                                "interaction.persistence_timeout",
                                attributes={
                                    "stage": "create",
                                    "timeout_ms": persistence_timeout_ms,
                                },
                            )
                            if not persistence_fail_open:
                                raise
                            raw_interaction_id = None

                        if raw_interaction_id:
                            interaction_id = unpack_mcp_result(raw_interaction_id)
                            inputs["interaction_id"] = interaction_id
                            interaction_persisted = True
                            inputs["interaction_persisted"] = True
                            # Also make interaction_id sticky
                            telemetry.update_current_trace({"interaction_id": interaction_id})
                            if telemetry.get_current_span():
                                telemetry.get_current_span().add_event(
                                    "persistence.create.success", {"interaction_id": interaction_id}
                                )
                        else:
                            interaction_persisted = False
                            inputs["interaction_persisted"] = False
                    except Exception as e:
                        # Structured logging with context (retry utility already logged attempts)
                        logger.error(
                            "Failed to create interaction after all retries",
                            extra={
                                "operation": "create_interaction",
                                "trace_id": thread_id,
                                "exception_type": type(e).__name__,
                                "exception_message": str(e),
                            },
                            exc_info=True,
                        )
                        if telemetry.get_current_span():
                            telemetry.get_current_span().add_event(
                                "persistence.create.failure",
                                {"exception": str(e), "type": type(e).__name__},
                            )
                        if telemetry.get_current_span():
                            telemetry.get_current_span().add_event(
                                "interaction.persist_failed",
                                {"stage": "create", "exception_type": type(e).__name__},
                            )
                        interaction_persisted = False
                        inputs["interaction_persisted"] = False
                        if persistence_mode == "strict":
                            raise RuntimeError(
                                f"Interaction creation failed (mode=strict): {e}"
                            ) from e
                        logger.warning(
                            "Continuing without interaction_id (best_effort)",
                            extra={"trace_id": thread_id},
                        )
                else:
                    logger.warning("create_interaction tool not available")
                    inputs["interaction_persisted"] = False

                # Execute workflow
                result = inputs.copy()
                try:
                    result = await app.ainvoke(inputs, config=config)
                    if "interaction_persisted" not in result:
                        result["interaction_persisted"] = interaction_persisted
                except Exception as execute_err:
                    logger.error(
                        "Critical error in agent workflow",
                        extra={
                            "trace_id": thread_id,
                            "interaction_id": interaction_id,
                            "exception_type": type(execute_err).__name__,
                        },
                        exc_info=True,
                    )
                    from agent.utils.llm_resilience import (
                        LLMCircuitOpenError,
                        LLMRateLimitExceededError,
                    )

                    if isinstance(execute_err, (LLMRateLimitExceededError, LLMCircuitOpenError)):
                        is_circuit_open = isinstance(execute_err, LLMCircuitOpenError)
                        error_message = "LLM rate limit exceeded. Please retry shortly."
                        error_code = "LLM_RATE_LIMIT_EXCEEDED"
                        details_safe = {
                            "llm_global_active": int(getattr(execute_err, "active_calls", 0)),
                            "llm_global_limit": int(getattr(execute_err, "limit", 0)),
                        }
                        if is_circuit_open:
                            error_message = "LLM circuit breaker is open. Please retry shortly."
                            error_code = "LLM_CIRCUIT_OPEN"
                            details_safe = {
                                "llm_circuit_failures": int(
                                    getattr(execute_err, "consecutive_failures", 0)
                                )
                            }

                        result["error"] = error_message
                        result["error_category"] = execute_err.category
                        result["retry_after_seconds"] = float(execute_err.retry_after_seconds)
                        result["error_metadata"] = {
                            "category": execute_err.category,
                            "code": error_code,
                            "message": error_message,
                            "is_retryable": True,
                            "retry_after_seconds": float(execute_err.retry_after_seconds),
                            "details_safe": details_safe,
                        }
                    else:
                        result["error"] = str(execute_err)
                        result["error_category"] = "SYSTEM_CRASH"
                    if "messages" not in result:
                        result["messages"] = []

                # 2. Update Interaction Logging (Post-execution) with retry
                if interaction_id:
                    update_tool = next((t for t in tools if t.name == "update_interaction"), None)
                    if update_tool:
                        try:
                            # Determine status
                            status = "SUCCESS"
                            if result.get("error"):
                                status = "FAILURE"
                            elif result.get("ambiguity_type"):
                                status = "CLARIFICATION_REQUIRED"

                            # Get last message as response
                            last_msg = ""
                            if result.get("messages") and len(result["messages"]) > 0:
                                last_message_obj = result["messages"][-1]
                                if hasattr(last_message_obj, "content"):
                                    last_msg = last_message_obj.content
                                else:
                                    last_msg = str(last_message_obj)

                            if not last_msg and result.get("error"):
                                last_msg = f"System Error: {result['error']}"

                            # Capture update payload for retry closure
                            update_payload = {
                                "interaction_id": interaction_id,
                                "tenant_id": tenant_id,
                                "generated_sql": result.get("current_sql"),
                                "response_payload": json.dumps(
                                    {"text": last_msg, "error": result.get("error")}
                                ),
                                "execution_status": status,
                                "error_type": result.get("error_category"),
                                "tables_used": result.get("table_names", []),
                            }

                            async def _update_interaction():
                                return await update_tool.ainvoke(update_payload, config=config)

                            if telemetry.get_current_span():
                                telemetry.get_current_span().add_event(
                                    "persistence.update.start", {"interaction_id": interaction_id}
                                )

                            persistence_timeout_ms = get_env_int(
                                "AGENT_INTERACTION_PERSISTENCE_TIMEOUT_MS", 500
                            )
                            persistence_fail_open = get_env_bool(
                                "AGENT_INTERACTION_PERSISTENCE_FAIL_OPEN", True
                            )

                            try:
                                await asyncio.wait_for(
                                    retry_with_backoff(
                                        _update_interaction,
                                        "update_interaction",
                                        extra_context={
                                            "trace_id": thread_id,
                                            "interaction_id": interaction_id,
                                        },
                                    ),
                                    timeout=persistence_timeout_ms / 1000.0,
                                )
                            except asyncio.TimeoutError:
                                logger.warning(
                                    "Interaction update timed out after %dms",
                                    persistence_timeout_ms,
                                )
                                telemetry.add_event(
                                    "interaction.persistence_timeout",
                                    attributes={
                                        "stage": "update",
                                        "timeout_ms": persistence_timeout_ms,
                                    },
                                )
                                if not persistence_fail_open:
                                    raise

                            if telemetry.get_current_span():
                                telemetry.get_current_span().add_event(
                                    "persistence.update.success", {"interaction_id": interaction_id}
                                )
                        except Exception as e:
                            if telemetry.get_current_span():
                                telemetry.get_current_span().add_event(
                                    "persistence.update.failure",
                                    {"interaction_id": interaction_id, "exception": str(e)},
                                )
                                telemetry.get_current_span().add_event(
                                    "interaction.persist_failed",
                                    {"stage": "update", "exception_type": type(e).__name__},
                                )
                            # Structured logging - update failure is observable
                            logger.error(
                                "Failed to update interaction after all retries",
                                extra={
                                    "operation": "update_interaction",
                                    "trace_id": thread_id,
                                    "interaction_id": interaction_id,
                                    "exception_type": type(e).__name__,
                                    "exception_message": str(e),
                                },
                                exc_info=True,
                            )
                            logger.error("Update failed for %s: %s", interaction_id, e)

                            # Mark result as having persistence failure (observable)
                            result["persistence_failed"] = True
                            result["persistence_error"] = str(e)
                            result["interaction_persisted"] = False
                            if persistence_mode == "strict":
                                raise RuntimeError(
                                    f"Interaction update failed (mode=strict): {e}"
                                ) from e
                    else:
                        logger.error("update_interaction tool not found in available tools")
                        logger.error("update_interaction tool missing!")
                        result["interaction_persisted"] = False

                llm_budget_state = current_budget_state()
                if llm_budget_state is not None:
                    llm_calls_total = int(getattr(llm_budget_state, "call_count", 0) or 0)
                    llm_token_total = int(getattr(llm_budget_state, "total", 0) or 0)
                result["llm_calls"] = int(llm_calls_total)
                result["llm_token_total"] = int(llm_token_total)

        # Metadata is already handled early and made sticky via telemetry_context

        # 3. Deterministic decision + retry summaries for debuggability
        decision_summary = build_decision_summary(result)
        retry_correction_summary = build_retry_correction_summary(result)
        run_decision_summary = build_run_decision_summary(
            result,
            llm_calls=llm_calls_total,
            llm_token_total=llm_token_total,
        )
        result["decision_summary"] = decision_summary
        result["retry_correction_summary"] = retry_correction_summary
        result["run_decision_summary"] = run_decision_summary
        try:
            get_run_summary_store().record(run_id=run_id, summary=run_decision_summary)
        except Exception:
            logger.warning("Failed to persist run decision summary", exc_info=True)

        retry_summary = result.get("retry_summary")
        if not isinstance(retry_summary, dict):
            retry_summary = {}
        retry_summary["correction_attempt_count"] = retry_correction_summary.get(
            "correction_attempt_count", 0
        )
        retry_summary["validation_failure_count"] = retry_correction_summary.get(
            "validation_failure_count", 0
        )
        retry_summary["final_stopping_reason"] = retry_correction_summary.get(
            "final_stopping_reason"
        )
        retry_summary["correction_attempts_truncated"] = bool(
            retry_correction_summary.get("correction_attempts_truncated", False)
        )
        retry_summary["validation_failures_truncated"] = bool(
            retry_correction_summary.get("validation_failures_truncated", False)
        )
        retry_summary["correction_attempts_dropped"] = int(
            retry_correction_summary.get("correction_attempts_dropped", 0) or 0
        )
        retry_summary["validation_failures_dropped"] = int(
            retry_correction_summary.get("validation_failures_dropped", 0) or 0
        )
        result["retry_summary"] = retry_summary
        validation_report = (
            result.get("validation_report")
            if isinstance(result.get("validation_report"), dict)
            else {}
        )
        from agent.utils.schema_cache import get_schema_refresh_collision_count

        schema_refresh_collisions = get_schema_refresh_collision_count()

        summary_attrs = {
            "decision.selected_tables_count": len(decision_summary.get("selected_tables", [])),
            "decision.rejected_tables_count": len(decision_summary.get("rejected_tables", [])),
            "decision.rejected_plan_candidates_count": len(
                decision_summary.get("rejected_plan_candidates", [])
            ),
            "decision.retry_count": int(decision_summary.get("retry_count", 0) or 0),
            "decision.schema_refresh_events": int(
                decision_summary.get("schema_refresh_events", 0) or 0
            ),
            "schema.refresh_collisions": int(schema_refresh_collisions),
            "query.join_count": int(
                decision_summary.get("query_complexity", {}).get("join_count", 0) or 0
            ),
            "query.estimated_table_count": int(
                decision_summary.get("query_complexity", {}).get("estimated_table_count", 0) or 0
            ),
            "query.estimated_scan_columns": int(
                decision_summary.get("query_complexity", {}).get("estimated_scan_columns", 0) or 0
            ),
            "query.detected_cartesian_flag": bool(
                decision_summary.get("query_complexity", {}).get("detected_cartesian_flag", False)
            ),
            "query.query_complexity_score": int(
                decision_summary.get("query_complexity", {}).get("query_complexity_score", 0) or 0
            ),
            "latency.retrieval_ms": float(
                decision_summary.get("latency_breakdown_ms", {}).get("retrieval_ms", 0.0) or 0.0
            ),
            "latency.planning_ms": float(
                decision_summary.get("latency_breakdown_ms", {}).get("planning_ms", 0.0) or 0.0
            ),
            "latency.generation_ms": float(
                decision_summary.get("latency_breakdown_ms", {}).get("generation_ms", 0.0) or 0.0
            ),
            "latency.validation_ms": float(
                decision_summary.get("latency_breakdown_ms", {}).get("validation_ms", 0.0) or 0.0
            ),
            "latency.execution_ms": float(
                decision_summary.get("latency_breakdown_ms", {}).get("execution_ms", 0.0) or 0.0
            ),
            "latency.correction_loop_ms": float(
                decision_summary.get("latency_breakdown_ms", {}).get("correction_loop_ms", 0.0)
                or 0.0
            ),
            "retry.correction_attempt_count": int(
                retry_correction_summary.get("correction_attempt_count", 0) or 0
            ),
            "retry.validation_failure_count": int(
                retry_correction_summary.get("validation_failure_count", 0) or 0
            ),
            "retry.final_stopping_reason": str(
                retry_correction_summary.get("final_stopping_reason") or "unknown"
            ),
            "retry.correction_attempts_truncated": bool(
                retry_correction_summary.get("correction_attempts_truncated", False)
            ),
            "retry.validation_failures_truncated": bool(
                retry_correction_summary.get("validation_failures_truncated", False)
            ),
            "retry.correction_attempts_dropped": int(
                retry_correction_summary.get("correction_attempts_dropped", 0) or 0
            ),
            "retry.validation_failures_dropped": int(
                retry_correction_summary.get("validation_failures_dropped", 0) or 0
            ),
            "validation.report.failed_rules_count": int(
                len(validation_report.get("failed_rules", []))
                if isinstance(validation_report.get("failed_rules"), list)
                else 0
            ),
            "validation.report.warnings_count": int(
                len(validation_report.get("warnings", []))
                if isinstance(validation_report.get("warnings"), list)
                else 0
            ),
            "validation.report.affected_tables_count": int(
                len(validation_report.get("affected_tables", []))
                if isinstance(validation_report.get("affected_tables"), list)
                else 0
            ),
            "run.retries": int(run_decision_summary.get("retries", 0) or 0),
            "run.llm_calls": int(run_decision_summary.get("llm_calls", 0) or 0),
            "run.llm_token_total": int(run_decision_summary.get("llm_token_total", 0) or 0),
            "run.schema_refresh_count": int(
                run_decision_summary.get("schema_refresh_count", 0) or 0
            ),
            "run.prefetch_discard_count": int(
                run_decision_summary.get("prefetch_discard_count", 0) or 0
            ),
            "run.error_categories_count": int(
                len(run_decision_summary.get("error_categories_encountered", []))
            ),
        }
        terminated_reason = run_decision_summary.get("terminated_reason")
        if terminated_reason:
            summary_attrs["run.terminated_reason"] = str(terminated_reason)
        record_query_complexity_score(
            decision_summary.get("query_complexity", {}).get("query_complexity_score", 0)
        )
        record_truncation_event(result.get("result_is_truncated", False))
        record_stage_latency_breakdown(decision_summary.get("latency_breakdown_ms"))
        telemetry.update_current_trace(summary_attrs)
        active_span = telemetry.get_current_span()
        if active_span:
            active_span.set_attributes(summary_attrs)
            active_span.add_event(
                "agent.decision_summary",
                {
                    "summary_json": json.dumps(
                        {
                            "decision_summary": decision_summary,
                            "retry_correction_summary": retry_correction_summary,
                            "run_decision_summary": run_decision_summary,
                        },
                        sort_keys=True,
                        default=str,
                    )[:2048]
                },
            )
            active_span.add_event(
                "agent.run_decision_summary",
                {
                    "summary_json": json.dumps(
                        run_decision_summary,
                        sort_keys=True,
                        default=str,
                    )[:2048]
                },
            )

        # 4. Final Flush (Control-plane safety)
        # Ensure traces are sent before returning to avoid loss on rapid process exit
        telemetry.flush(timeout_ms=500)

        return result
