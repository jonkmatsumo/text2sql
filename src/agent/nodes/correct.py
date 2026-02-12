"""SQL correction node for self-healing queries with MLflow tracing.

Enhanced with error taxonomy for targeted correction strategies.
"""

import logging
import time

from dotenv import load_dotenv
from langchain_core.prompts import ChatPromptTemplate

from agent.state import AgentState
from agent.taxonomy.error_taxonomy import classify_error, generate_correction_strategy
from agent.telemetry import telemetry
from agent.telemetry_schema import SpanKind, TelemetryKeys
from agent.utils.budgeting import update_latency_ema
from agent.utils.prompt_budget import consume_prompt_budget
from common.config.env import get_env_bool, get_env_float, get_env_int, get_env_str

logger = logging.getLogger(__name__)

load_dotenv()


def _append_bounded_correction_event(
    state: AgentState, event: dict
) -> tuple[list[dict], bool, int]:
    max_events = get_env_int("AGENT_RETRY_SUMMARY_MAX_EVENTS", 20) or 20
    max_events = max(1, int(max_events))
    # Character limit for the entire list of dicts serialized
    max_chars = get_env_int("AGENT_RETRY_SUMMARY_MAX_CHARS", 10000) or 10000

    existing_truncated = bool(state.get("correction_attempts_truncated"))
    existing_dropped_raw = state.get("correction_attempts_dropped", 0)
    existing_dropped = (
        int(existing_dropped_raw) if isinstance(existing_dropped_raw, (int, float)) else 0
    )
    events = state.get("correction_attempts") or []
    if not isinstance(events, list):
        events = []
    events = [entry for entry in events if isinstance(entry, dict)]

    # 1. Append new event
    events.append(event)

    # 2. Bound by item count (FIFO)
    dropped_now = 0
    if len(events) > max_events:
        dropped_now = len(events) - max_events
        events = events[dropped_now:]

    # 3. Bound by character count (FIFO)
    # Estimate size by JSON serialization
    import json

    def _estimate_size(evs):
        return len(json.dumps(evs))

    while events and _estimate_size(events) > max_chars:
        if len(events) == 1:
            # If a single event is too large, we can't easily truncate the dict
            # without breaking schema, so we just drop it if it's over the limit.
            events = []
            dropped_now += 1
            break
        events.pop(0)
        dropped_now += 1

    total_dropped = existing_dropped + dropped_now
    return events, (existing_truncated or dropped_now > 0), total_dropped


def correct_sql_node(state: AgentState) -> dict:
    """
    Node: CorrectSQL.

    Analyzes the error using taxonomy classification and generates targeted fixes.
    This implements the self-correction loop with structured error feedback.

    Features:
    - Error classification using taxonomy patterns
    - Targeted correction strategies (not blind regeneration)
    - Consumption of AST validation feedback
    - Correction plan tracking for observability

    Args:
        state: Current agent state with error and current_sql

    Returns:
        dict: Updated state with corrected SQL, error_category, and incremented retry_count
    """
    with telemetry.start_span(
        name="correct_sql",
        span_type=SpanKind.AGENT_NODE,
    ) as span:
        span.set_attribute(TelemetryKeys.EVENT_TYPE, SpanKind.AGENT_NODE)
        span.set_attribute(TelemetryKeys.EVENT_NAME, "correct_sql")
        error = state.get("error")
        current_sql = state.get("current_sql")
        schema_context = state.get("schema_context", "")
        retry_count = state.get("retry_count", 0)
        retry_after_seconds = state.get("retry_after_seconds")
        procedural_plan = state.get("procedural_plan", "")
        ast_validation_result = state.get("ast_validation_result")
        existing_loop_ms_raw = state.get("latency_correction_loop_ms")
        existing_loop_ms = (
            float(existing_loop_ms_raw) if isinstance(existing_loop_ms_raw, (int, float)) else 0.0
        )

        def _correction_loop_payload(attempt_latency_seconds: float | None = None) -> dict:
            total_ms = max(0.0, existing_loop_ms)
            if attempt_latency_seconds is not None:
                total_ms += max(0.0, float(attempt_latency_seconds) * 1000.0)
            span.set_attribute("latency.correction_loop_ms", total_ms)
            return {"latency_correction_loop_ms": total_ms}

        span.set_inputs(
            {
                "error": error,
                "current_sql": current_sql,
                "retry_count": retry_count,
            }
        )

        retry = retry_count + 1

        if retry_after_seconds is not None and float(retry_after_seconds) > 0:
            sleep_seconds = float(retry_after_seconds)
            deadline_ts = state.get("deadline_ts")
            if deadline_ts is not None:
                sleep_seconds = min(sleep_seconds, max(0.0, deadline_ts - time.monotonic()))
            if sleep_seconds > 0:
                span.set_attribute("retry.retry_after_seconds", sleep_seconds)
                span.add_event("retry.retry_after_sleep", {"sleep_seconds": sleep_seconds})
                time.sleep(sleep_seconds)

        # Classify the error using taxonomy
        # Prioritize structured error category from state (higher confidence)
        from agent.taxonomy.error_taxonomy import ERROR_TAXONOMY

        state_error_category = state.get("error_category")
        if state_error_category and state_error_category in ERROR_TAXONOMY:
            error_category = state_error_category
            category_info = ERROR_TAXONOMY[error_category]
        else:
            error_category, category_info = classify_error(error or "")

        correction_event = {
            "attempt": int(retry),
            "error_category": str(error_category),
            "has_retry_after": bool(retry_after_seconds),
        }
        (
            correction_attempts,
            correction_attempts_truncated,
            correction_attempts_dropped,
        ) = _append_bounded_correction_event(state, correction_event)

        # Terminal Error Gating (Phase C)
        if error_category in ("TABLE_INACCESSIBLE", "auth"):
            span.set_attribute("retry.terminated_early", True)
            span.set_attribute("retry.terminal_category", error_category)
            correction_event["outcome"] = "terminal_stop"
            return {
                "error": f"Terminal error: {error_category}. No correction possible.",
                "error_category": error_category,
                "retry_after_seconds": None,
                "correction_attempts": correction_attempts,
                "correction_attempts_truncated": correction_attempts_truncated,
                "correction_attempts_dropped": correction_attempts_dropped,
                **_correction_loop_payload(),
            }

        span.set_attribute("error_category", error_category)
        max_attempts = 3
        span.set_attribute("retry.attempt", retry)
        span.set_attribute("retry.max_attempts", max_attempts)
        span.set_attribute("retry.reason_category", error_category)

        if telemetry.get_current_span():
            telemetry.get_current_span().add_event(
                "agent.retry",
                {
                    "stage": "correct_sql",
                    "reason_category": error_category,
                    "attempt": retry,
                    "max_attempts": max_attempts,
                    "provider": get_env_str("QUERY_TARGET_BACKEND", "postgres"),
                },
            )

        # Generate targeted correction strategy
        correction_strategy = generate_correction_strategy(
            error_message=error or "",
            failed_sql=current_sql or "",
            schema_context=schema_context,
            missing_identifiers=state.get("missing_identifiers"),
            error_metadata=state.get("error_metadata"),
        )

        # Prepare context variables for the prompt
        taxonomy_context = f"""
Additional Context:
- Error Category: {category_info.name}
- Correction Strategy: {category_info.strategy}
"""

        plan_context = ""
        if procedural_plan:
            plan_context = f"""
Original Query Plan:
{procedural_plan}

Ensure your correction follows the original plan's intent.
"""

        ast_context = ""
        if ast_validation_result and not ast_validation_result.get("is_valid"):
            violations = ast_validation_result.get("violations", [])
            if violations:
                violation_details = "\n".join(
                    f"- [{v.get('violation_type')}] {v.get('message')}" for v in violations
                )
                ast_context = f"""
AST Validation Violations:
{violation_details}

Address these specific violations in your correction.
"""

        # Define system template with named placeholders
        system_template = """You are a PostgreSQL expert specializing in error correction.

{correction_strategy}

{taxonomy_context}
{plan_context}
{ast_context}
"""

        prompt = ChatPromptTemplate.from_messages(
            [
                ("system", system_template),
                (
                    "user",
                    """Schema Context:
{schema_context}

Failed Query:
{bad_query}

Error Message:
{error_msg}

Return ONLY the corrected SQL query. No markdown, no explanations.""",
                ),
            ]
        )

        from agent.utils.budget import TokenBudget

        budget = TokenBudget.from_dict(state.get("token_budget"))
        prompt_bytes_used = int(state.get("llm_prompt_bytes_used") or 0)
        if budget and budget.is_exhausted():
            span.set_attribute("budget.exhausted", True)
            correction_event["outcome"] = "budget_exhausted"
            return {
                "error": "Token budget exhausted during correction loop.",
                "error_category": "budget_exhausted",
                "retry_after_seconds": None,
                "correction_attempts": correction_attempts,
                "correction_attempts_truncated": correction_attempts_truncated,
                "correction_attempts_dropped": correction_attempts_dropped,
                "llm_prompt_bytes_used": prompt_bytes_used,
                "llm_budget_exceeded": bool(state.get("llm_budget_exceeded", False)),
                **_correction_loop_payload(),
            }

        # Detect repeated error signatures to stop infinite fails
        from common.utils.hashing import canonical_json_hash

        error_msg_norm = (error or "").strip().lower()
        # Simple normalization: remove very platform-specific bits if needed,
        # but here simple trim is okay
        sig_data = {"category": error_category, "message": error_msg_norm}
        current_sig = canonical_json_hash(sig_data)

        signatures = state.get("error_signatures", [])
        if current_sig in signatures:
            span.set_attribute("retry.stopped_due_to_repeat", True)
            span.set_attribute("retry.repeated_signature", current_sig)
            correction_event["outcome"] = "repeated_error"
            return {
                "error": f"Stopping correction loop: Repeated error detected ({error_category})",
                "error_category": "repeated_error",
                "retry_after_seconds": None,
                "correction_attempts": correction_attempts,
                "correction_attempts_truncated": correction_attempts_truncated,
                "correction_attempts_dropped": correction_attempts_dropped,
                **_correction_loop_payload(),
            }

        updated_signatures = signatures + [current_sig]

        from agent.llm_client import get_llm
        from agent.utils.llm_run_budget import LLMBudgetExceededError, budget_telemetry_attributes
        from agent.utils.sql_similarity import compute_sql_similarity

        # Internal loop for drift detection
        max_drift_retries = 1
        drift_attempts = 0

        # Initial context

        current_error_msg = error or ""

        while True:
            # Re-construct prompt with potentially updated context (if drifting)
            # We reuse the static contexts (taxonomy, plan, ast) but error_msg might change

            # If we are retrying due to drift, append warning.
            # Ideally we append to system or user. Append to user prompt for visibility.

            (
                prompt_bytes_used,
                prompt_bytes_increment,
                prompt_budget_exceeded,
                prompt_bytes_limit,
            ) = consume_prompt_budget(
                prompt_bytes_used,
                {
                    "correction_strategy": correction_strategy,
                    "taxonomy_context": taxonomy_context,
                    "plan_context": plan_context,
                    "ast_context": ast_context,
                    "schema_context": schema_context,
                    "bad_query": current_sql,
                    "error_msg": current_error_msg,
                },
            )
            span.set_attribute("llm.prompt_bytes.increment", prompt_bytes_increment)
            span.set_attribute("llm.prompt_bytes.used", prompt_bytes_used)
            span.set_attribute("llm.prompt_bytes.limit", prompt_bytes_limit)
            span.set_attribute("llm.prompt_bytes.exceeded", prompt_budget_exceeded)
            if prompt_budget_exceeded:
                correction_event["outcome"] = "prompt_budget_exhausted"
                return {
                    "error": "LLM prompt budget exceeded during correction loop.",
                    "error_category": "budget_exhausted",
                    "retry_after_seconds": None,
                    "correction_attempts": correction_attempts,
                    "correction_attempts_truncated": correction_attempts_truncated,
                    "correction_attempts_dropped": correction_attempts_dropped,
                    "llm_prompt_bytes_used": prompt_bytes_used,
                    "llm_budget_exceeded": True,
                    "error_metadata": {
                        "reason_code": "llm_prompt_budget_exceeded",
                        "llm_prompt_bytes_used": prompt_bytes_used,
                        "llm_prompt_bytes_limit": prompt_bytes_limit,
                        "is_retryable": False,
                    },
                    **_correction_loop_payload(),
                }

            chain = prompt | get_llm(temperature=0, seed=state.get("seed"))

            start_time = time.monotonic()
            try:
                response = chain.invoke(
                    {
                        "correction_strategy": correction_strategy,
                        "taxonomy_context": taxonomy_context,
                        "plan_context": plan_context,
                        "ast_context": ast_context,
                        "schema_context": schema_context,
                        "bad_query": current_sql,
                        "error_msg": current_error_msg,
                    }
                )
            except LLMBudgetExceededError as budget_error:
                correction_event["outcome"] = "token_budget_exceeded"
                span.set_attribute("budget.exhausted", True)
                span.set_attribute("llm.budget.exceeded", True)
                span.set_attributes(budget_telemetry_attributes(budget_error.state))
                return {
                    "error": "LLM token budget exceeded during correction loop.",
                    "error_category": "budget_exceeded",
                    "retry_after_seconds": None,
                    "correction_attempts": correction_attempts,
                    "correction_attempts_truncated": correction_attempts_truncated,
                    "correction_attempts_dropped": correction_attempts_dropped,
                    "llm_budget_exceeded": True,
                    "error_metadata": {
                        "reason_code": "llm_token_budget_exceeded",
                        "is_retryable": False,
                        "llm_tokens_prompt_total": int(budget_error.state.prompt_total),
                        "llm_tokens_completion_total": int(budget_error.state.completion_total),
                        "llm_tokens_budget_limit": int(budget_error.state.max_tokens),
                        "llm_tokens_requested": int(budget_error.requested_tokens),
                    },
                    **_correction_loop_payload(),
                }
            latency_seconds = time.monotonic() - start_time

            # Capture token usage
            from agent.llm_client import extract_token_usage

            usage_stats = extract_token_usage(response)
            if usage_stats:
                span.set_attributes(usage_stats)

            # Extract SQL
            corrected_sql = response.content.strip()
            if corrected_sql.startswith("```sql"):
                corrected_sql = corrected_sql[6:]
            if corrected_sql.startswith("```"):
                corrected_sql = corrected_sql[3:]
            if corrected_sql.endswith("```"):
                corrected_sql = corrected_sql[:-3]
            corrected_sql = corrected_sql.strip()

            # Similarity Check
            should_enforce = get_env_bool("AGENT_CORRECTION_SIMILARITY_ENFORCE", False)
            if should_enforce and current_sql:
                min_score = get_env_float("AGENT_CORRECTION_SIMILARITY_MIN_SCORE", 0.5)
                similarity = compute_sql_similarity(current_sql, corrected_sql)
                span.set_attribute("correction.similarity.score", similarity)

                if similarity < min_score:
                    span.set_attribute("correction.similarity.rejected", True)
                    logger.warning(f"Correction rejected due to drift. Sim: {similarity}")

                    if drift_attempts < max_drift_retries:
                        drift_attempts += 1
                        current_error_msg = (
                            "Previous correction rejected due to structural drift "
                            f"(sim {similarity:.2f} < {min_score}). "
                            "Keep table references/structure close to original."
                        )
                        continue  # Retry
                    else:
                        # Retries exhausted, return drift error or just fail?
                        # Request says "reject correction attempt and return a targeted violation".
                        # If we return the drifted SQL, it will execute and likely fail or be wrong.
                        # If we return original SQL, validation fails.
                        # Return original SQL + error category.
                        msg = (
                            "Correction failed: Structural drift detected "
                            f"(score {similarity:.2f})"
                        )
                        correction_event["outcome"] = "rejected_drift"
                        correction_event["similarity_score"] = round(float(similarity), 4)
                        return {
                            "current_sql": current_sql,
                            "retry_count": retry,
                            "error": msg,
                            "error_category": "correction_drift",
                            "correction_plan": correction_strategy,
                            "latency_correct_seconds": latency_seconds,
                            "ema_llm_latency_seconds": None,  # Don't update EMA on fail
                            "retry_after_seconds": None,
                            "correction_attempts": correction_attempts,
                            "correction_attempts_truncated": correction_attempts_truncated,
                            "correction_attempts_dropped": correction_attempts_dropped,
                            "llm_prompt_bytes_used": prompt_bytes_used,
                            "llm_budget_exceeded": False,
                            **_correction_loop_payload(latency_seconds),
                        }
                else:
                    span.set_attribute("correction.similarity.rejected", False)

            # Success path (no enforcement or passed check)
            break

        span.set_attribute("latency.correct_seconds", latency_seconds)
        ema_alpha = get_env_float("AGENT_RETRY_BUDGET_EMA_ALPHA", 0.3)
        ema_latency = update_latency_ema(
            state.get("ema_llm_latency_seconds"), latency_seconds, ema_alpha, dampen=True
        )
        span.set_attribute("retry.budget.observed_latency_seconds", latency_seconds)
        if ema_latency is not None:
            span.set_attribute("retry.budget.ema_latency_seconds", ema_latency)

        if usage_stats and budget:
            tokens = usage_stats.get("llm.token_usage.total_tokens", 0)
            budget.consume(tokens)
            span.set_attribute("budget.consumed_in_step", tokens)
            span.set_attribute("budget.total_consumed", budget.consumed_tokens)

        span.set_outputs(
            {
                "corrected_sql": corrected_sql,
                "retry_count": retry,
                "error_category": error_category,
            }
        )
        correction_event["outcome"] = "corrected"

        return {
            "current_sql": corrected_sql,
            "retry_count": retry,
            "error": None,  # Reset error for next attempt
            "error_category": error_category,
            "correction_plan": correction_strategy,
            "latency_correct_seconds": latency_seconds,
            "ema_llm_latency_seconds": ema_latency,
            "retry_after_seconds": None,
            "token_budget": budget.to_dict() if budget else state.get("token_budget"),
            "llm_prompt_bytes_used": prompt_bytes_used,
            "llm_budget_exceeded": False,
            "error_signatures": updated_signatures,
            "correction_attempts": correction_attempts,
            "correction_attempts_truncated": correction_attempts_truncated,
            "correction_attempts_dropped": correction_attempts_dropped,
            **_correction_loop_payload(latency_seconds),
        }
