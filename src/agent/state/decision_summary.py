"""Deterministic summary builders for agent decision transparency.

Semantics captured here are intentionally stable for operator/debug tooling:

- Query complexity score is a relative heuristic from AST metadata:
  `(join_count * 3) + (estimated_table_count * 2) + (union_count * 4)`.
  It is not a cost-based optimizer estimate.
- `latency_breakdown_ms` fields are stage-local timings and may not sum exactly
  to end-to-end request latency due to orchestration overhead.
- `rejected_plan_candidates[].reason_code` uses canonical low-cardinality values
  from `RejectedPlanCandidateReason` with `unknown` fallback.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from agent.state.decision_events import summarize_decision_events
from common.config.env import get_env_int
from common.constants.reason_codes import RejectedPlanCandidateReason
from common.sanitization.text import redact_sensitive_info

_MAX_REASON_TEXT_LENGTH = 96


def _bounded_events(events: Any, max_items: int) -> list[dict]:
    if not isinstance(events, list):
        return []
    normalized = [event for event in events if isinstance(event, dict)]
    if max_items <= 0:
        return []
    return normalized[:max_items]


def _normalize_table_name(raw: Any) -> str:
    if not isinstance(raw, str):
        return ""
    return raw.strip().lower()


def _normalize_reason_text(raw: Any) -> str:
    redacted = redact_sensitive_info(str(raw or "")).strip().lower()
    if not redacted:
        return RejectedPlanCandidateReason.UNKNOWN.value
    return redacted[:_MAX_REASON_TEXT_LENGTH]


def _as_state_mapping(state: Any) -> dict[str, Any]:
    if isinstance(state, dict):
        return state
    if isinstance(state, Mapping):
        return dict(state)
    return {}


def _resolve_final_stopping_reason(state: dict[str, Any]) -> str:
    retry_reason = state.get("retry_reason")
    if isinstance(retry_reason, str) and retry_reason.strip():
        return retry_reason.strip()

    termination_reason = state.get("termination_reason")
    if termination_reason is not None:
        return str(termination_reason)

    error_category = state.get("error_category")
    if isinstance(error_category, str) and error_category.strip():
        return error_category.strip()

    if state.get("error"):
        return "error"

    return "success"


def build_retry_correction_summary(
    state: dict[str, Any], *, max_events: int | None = None
) -> dict[str, Any]:
    """Build a bounded summary of retry/correction behavior."""
    normalized_state = _as_state_mapping(state)
    max_items = max_events
    if max_items is None:
        max_items = get_env_int("AGENT_RETRY_SUMMARY_MAX_EVENTS", 20) or 20
    max_items = max(1, int(max_items))

    raw_corrections = normalized_state.get("correction_attempts") or []
    raw_validation_failures = normalized_state.get("validation_failures") or []
    corrections = _bounded_events(raw_corrections, max_items)
    validation_failures = _bounded_events(raw_validation_failures, max_items)
    correction_truncated = bool(normalized_state.get("correction_attempts_truncated")) or (
        len(corrections) < len(raw_corrections) if isinstance(raw_corrections, list) else False
    )
    validation_truncated = bool(normalized_state.get("validation_failures_truncated")) or (
        len(validation_failures) < len(raw_validation_failures)
        if isinstance(raw_validation_failures, list)
        else False
    )
    correction_dropped = int(normalized_state.get("correction_attempts_dropped", 0) or 0)
    validation_dropped = int(normalized_state.get("validation_failures_dropped", 0) or 0)

    return {
        "correction_attempt_count": (
            len(raw_corrections) if isinstance(raw_corrections, list) else 0
        ),
        "validation_failure_count": (
            len(raw_validation_failures) if isinstance(raw_validation_failures, list) else 0
        ),
        "correction_attempts": corrections,
        "validation_failures": validation_failures,
        "correction_attempts_truncated": correction_truncated,
        "validation_failures_truncated": validation_truncated,
        "correction_attempts_dropped": correction_dropped,
        "validation_failures_dropped": validation_dropped,
        "final_stopping_reason": _resolve_final_stopping_reason(normalized_state),
    }


def _collect_rejected_tables(state: dict[str, Any], max_tables: int) -> list[dict[str, str]]:
    rejected: dict[tuple[str, str], dict[str, str]] = {}

    validation_failures = state.get("validation_failures") or []
    if isinstance(validation_failures, list):
        for entry in validation_failures:
            if not isinstance(entry, dict):
                continue
            for rejected_table in entry.get("rejected_tables") or []:
                if not isinstance(rejected_table, dict):
                    continue
                table = _normalize_table_name(rejected_table.get("table"))
                reason = _normalize_reason_text(rejected_table.get("reason"))
                if not table:
                    continue
                rejected[(table, reason)] = {"table": table, "reason": reason}

    ast_result = state.get("ast_validation_result")
    if isinstance(ast_result, dict):
        for violation in ast_result.get("violations") or []:
            if not isinstance(violation, dict):
                continue
            details = violation.get("details")
            if not isinstance(details, dict):
                continue
            reason = _normalize_reason_text(details.get("reason"))
            tables = details.get("tables")
            if isinstance(tables, list):
                for table in tables:
                    normalized = _normalize_table_name(table)
                    if normalized:
                        rejected[(normalized, reason)] = {
                            "table": normalized,
                            "reason": reason,
                        }
                continue
            table = _normalize_table_name(details.get("table"))
            if table:
                rejected[(table, reason)] = {"table": table, "reason": reason}

    ordered = [rejected[key] for key in sorted(rejected.keys())]
    return ordered[:max_tables]


def _reason_to_reason_code(reason: Any) -> str:
    raw = _normalize_reason_text(reason)
    if not raw:
        return RejectedPlanCandidateReason.UNKNOWN.value
    if "allowlist" in raw:
        return RejectedPlanCandidateReason.ALLOWLIST.value
    if "schema" in raw or "missing" in raw or "not_found" in raw or "unknown_table" in raw:
        return RejectedPlanCandidateReason.SCHEMA_MISMATCH.value
    if "similarity" in raw or "threshold" in raw:
        return RejectedPlanCandidateReason.SIMILARITY_THRESHOLD.value
    return RejectedPlanCandidateReason.VALIDATION_RULE.value


def _collect_rejected_plan_candidates(
    state: dict[str, Any],
    selected_tables: list[str],
    max_candidates: int,
) -> list[dict[str, str]]:
    selected = {_normalize_table_name(table) for table in selected_tables}
    selected.discard("")
    rejected: dict[tuple[str, str], dict[str, str]] = {}

    raw_candidates = state.get("table_names") or []
    if isinstance(raw_candidates, list):
        for raw_candidate in raw_candidates:
            table = _normalize_table_name(raw_candidate)
            if not table or table in selected:
                continue
            rejected[(table, RejectedPlanCandidateReason.SIMILARITY_THRESHOLD.value)] = {
                "table": table,
                "reason_code": RejectedPlanCandidateReason.SIMILARITY_THRESHOLD.value,
            }

    for rejected_table in _collect_rejected_tables(state, max(max_candidates * 2, 50)):
        table = _normalize_table_name(rejected_table.get("table"))
        if not table:
            continue
        reason_code = _reason_to_reason_code(rejected_table.get("reason"))
        rejected[(table, reason_code)] = {
            "table": table,
            "reason_code": reason_code,
        }

    ordered = [rejected[key] for key in sorted(rejected.keys())]
    return ordered[:max_candidates]


def _extract_query_complexity(state: dict[str, Any]) -> dict[str, Any]:
    ast_result = state.get("ast_validation_result")
    metadata = ast_result.get("metadata") if isinstance(ast_result, dict) else {}
    if not isinstance(metadata, dict):
        metadata = {}

    return {
        "join_count": int(state.get("query_join_count") or metadata.get("join_count") or 0),
        "estimated_table_count": int(
            state.get("query_estimated_table_count") or metadata.get("estimated_table_count") or 0
        ),
        "estimated_scan_columns": int(
            state.get("query_estimated_scan_columns") or metadata.get("estimated_scan_columns") or 0
        ),
        "union_count": int(state.get("query_union_count") or metadata.get("union_count") or 0),
        "detected_cartesian_flag": bool(
            state.get("query_detected_cartesian_flag")
            or metadata.get("detected_cartesian_flag")
            or False
        ),
        "query_complexity_score": int(
            state.get("query_complexity_score") or metadata.get("query_complexity_score") or 0
        ),
    }


def _extract_latency_breakdown(state: dict[str, Any]) -> dict[str, float]:
    """Extract stage-local latencies in milliseconds from state.

    Each value is independently measured around a pipeline stage. Values are
    non-negative and deterministic for the captured state snapshot.
    """

    def _as_ms(value: Any) -> float:
        if isinstance(value, (int, float)):
            return max(0.0, float(value))
        return 0.0

    return {
        "retrieval_ms": _as_ms(state.get("latency_retrieval_ms")),
        "planning_ms": _as_ms(state.get("latency_planning_ms")),
        "generation_ms": _as_ms(state.get("latency_generation_ms")),
        "validation_ms": _as_ms(state.get("latency_validation_ms")),
        "execution_ms": _as_ms(state.get("latency_execution_ms")),
        "correction_loop_ms": _as_ms(state.get("latency_correction_loop_ms")),
    }


def build_decision_summary(
    state: dict[str, Any], *, max_tables: int | None = None
) -> dict[str, Any]:
    """Build a deterministic high-level decision summary from state."""
    normalized_state = _as_state_mapping(state)
    table_limit = max_tables
    if table_limit is None:
        table_limit = get_env_int("AGENT_DECISION_SUMMARY_MAX_TABLES", 50) or 50
    table_limit = max(1, int(table_limit))

    selected_tables: list[str] = []
    raw_tables = normalized_state.get("table_lineage")
    if not isinstance(raw_tables, list) or not raw_tables:
        ast_result = normalized_state.get("ast_validation_result")
        if isinstance(ast_result, dict):
            metadata = ast_result.get("metadata")
            if isinstance(metadata, dict) and isinstance(metadata.get("table_lineage"), list):
                raw_tables = metadata.get("table_lineage")
    if not isinstance(raw_tables, list) or not raw_tables:
        raw_tables = normalized_state.get("table_names") or []
    if isinstance(raw_tables, list):
        seen = set()
        for table in raw_tables:
            normalized = _normalize_table_name(table)
            if normalized and normalized not in seen:
                seen.add(normalized)
                selected_tables.append(normalized)
        selected_tables.sort()

    rejected_tables = _collect_rejected_tables(normalized_state, table_limit)
    rejected_plan_candidates = _collect_rejected_plan_candidates(
        normalized_state,
        selected_tables,
        table_limit,
    )

    return {
        "selected_tables": selected_tables[:table_limit],
        "rejected_tables": rejected_tables,
        "rejected_plan_candidates": rejected_plan_candidates,
        "retry_count": int(normalized_state.get("retry_count", 0) or 0),
        "schema_refresh_events": int(normalized_state.get("schema_refresh_count", 0) or 0),
        "query_complexity": _extract_query_complexity(normalized_state),
        "latency_breakdown_ms": _extract_latency_breakdown(normalized_state),
    }


def _collect_error_categories_encountered(state: dict[str, Any]) -> list[str]:
    categories: set[str] = set()

    def _add_category(raw: Any) -> None:
        if not isinstance(raw, str):
            return
        normalized = raw.strip().lower()
        if normalized:
            categories.add(normalized)

    _add_category(state.get("error_category"))
    error_metadata = state.get("error_metadata")
    if isinstance(error_metadata, dict):
        _add_category(error_metadata.get("category"))

    correction_attempts = state.get("correction_attempts") or []
    if isinstance(correction_attempts, list):
        for event in correction_attempts:
            if isinstance(event, dict):
                _add_category(event.get("error_category"))

    return sorted(categories)


def build_run_decision_summary(
    state: dict[str, Any], *, llm_calls: int | None = None, llm_token_total: int | None = None
) -> dict[str, Any]:
    """Build a final run-level summary artifact for postmortem/debug workflows."""
    normalized_state = _as_state_mapping(state)
    resolved_llm_calls = (
        int(llm_calls) if llm_calls is not None else int(normalized_state.get("llm_calls", 0) or 0)
    )
    resolved_llm_token_total = (
        int(llm_token_total)
        if llm_token_total is not None
        else int(normalized_state.get("llm_token_total", 0) or 0)
    )

    return {
        "tenant_id": normalized_state.get("tenant_id"),
        "replay_mode": bool(normalized_state.get("replay_mode", False)),
        "schema_snapshot_id": (
            normalized_state.get("schema_snapshot_id")
            or normalized_state.get("pinned_schema_snapshot_id")
        ),
        "retries": int(normalized_state.get("retry_count", 0) or 0),
        "llm_calls": max(0, resolved_llm_calls),
        "llm_token_total": max(0, resolved_llm_token_total),
        "schema_refresh_count": int(normalized_state.get("schema_refresh_count", 0) or 0),
        "prefetch_discard_count": int(normalized_state.get("prefetch_discard_count", 0) or 0),
        "kill_switches": {
            "disable_prefetch": bool(normalized_state.get("prefetch_kill_switch_enabled", False)),
            "disable_schema_refresh": bool(
                normalized_state.get("schema_refresh_kill_switch_enabled", False)
            ),
            "disable_llm_retries": bool(
                normalized_state.get("llm_retries_kill_switch_enabled", False)
            ),
        },
        "decision_event_counts": summarize_decision_events(normalized_state),
        "decision_events_truncated": bool(normalized_state.get("decision_events_truncated", False)),
        "decision_events_dropped": int(normalized_state.get("decision_events_dropped", 0) or 0),
        "error_categories_encountered": _collect_error_categories_encountered(normalized_state),
        "terminated_reason": _resolve_final_stopping_reason(normalized_state),
    }
