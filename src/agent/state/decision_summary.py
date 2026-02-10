"""Deterministic summary builders for agent decision transparency."""

from __future__ import annotations

from typing import Any

from common.config.env import get_env_int


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
    max_items = max_events
    if max_items is None:
        max_items = get_env_int("AGENT_RETRY_SUMMARY_MAX_EVENTS", 20) or 20
    max_items = max(1, int(max_items))

    raw_corrections = state.get("correction_attempts") or []
    raw_validation_failures = state.get("validation_failures") or []
    corrections = _bounded_events(raw_corrections, max_items)
    validation_failures = _bounded_events(raw_validation_failures, max_items)

    return {
        "correction_attempt_count": (
            len(raw_corrections) if isinstance(raw_corrections, list) else 0
        ),
        "validation_failure_count": (
            len(raw_validation_failures) if isinstance(raw_validation_failures, list) else 0
        ),
        "correction_attempts": corrections,
        "validation_failures": validation_failures,
        "final_stopping_reason": _resolve_final_stopping_reason(state),
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
                reason = str(rejected_table.get("reason") or "unknown").strip().lower()
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
            reason = str(details.get("reason") or "unknown").strip().lower()
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


def build_decision_summary(
    state: dict[str, Any], *, max_tables: int | None = None
) -> dict[str, Any]:
    """Build a deterministic high-level decision summary from state."""
    table_limit = max_tables
    if table_limit is None:
        table_limit = get_env_int("AGENT_DECISION_SUMMARY_MAX_TABLES", 50) or 50
    table_limit = max(1, int(table_limit))

    selected_tables: list[str] = []
    raw_tables = state.get("table_names") or []
    if isinstance(raw_tables, list):
        seen = set()
        for table in raw_tables:
            normalized = _normalize_table_name(table)
            if normalized and normalized not in seen:
                seen.add(normalized)
                selected_tables.append(normalized)
        selected_tables.sort()

    rejected_tables = _collect_rejected_tables(state, table_limit)

    return {
        "selected_tables": selected_tables[:table_limit],
        "rejected_tables": rejected_tables,
        "retry_count": int(state.get("retry_count", 0) or 0),
        "schema_refresh_events": int(state.get("schema_refresh_count", 0) or 0),
    }
