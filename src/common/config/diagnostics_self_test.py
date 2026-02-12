"""Safe self-test helpers for operator diagnostics."""

from __future__ import annotations

from typing import Any


def run_diagnostics_self_test() -> dict[str, Any]:
    """Run bounded, fake-only checks for validation and execution envelope paths."""
    report: dict[str, Any] = {"status": "ok"}

    try:
        from agent.validation.ast_validator import validate_sql

        validation_result = validate_sql("SELECT 1 AS health_check")
        validation_ok = bool(validation_result.is_valid)
        report["validation"] = {
            "status": "ok" if validation_ok else "degraded",
            "is_valid": validation_ok,
            "violation_count": len(validation_result.violations),
            "warning_count": len(validation_result.warnings),
        }
    except Exception as exc:  # pragma: no cover - defensive fallback
        report["validation"] = {"status": "error", "message": str(exc)[:256]}

    try:
        from common.models.tool_envelopes import (
            ExecuteSQLQueryMetadata,
            ExecuteSQLQueryResponseEnvelope,
            parse_execute_sql_response,
        )

        envelope = ExecuteSQLQueryResponseEnvelope(
            rows=[{"health_check": 1}],
            metadata=ExecuteSQLQueryMetadata(rows_returned=1, is_truncated=False),
        )
        parsed = parse_execute_sql_response(envelope.model_dump())
        execution_ok = (
            bool(parsed.rows) and parsed.metadata.rows_returned == 1 and not parsed.is_error()
        )
        report["execution"] = {
            "status": "ok" if execution_ok else "degraded",
            "rows_returned": int(parsed.metadata.rows_returned or 0),
            "envelope_parse_ok": execution_ok,
        }
    except Exception as exc:  # pragma: no cover - defensive fallback
        report["execution"] = {"status": "error", "message": str(exc)[:256]}

    component_statuses = [
        str(report.get("validation", {}).get("status", "error")),
        str(report.get("execution", {}).get("status", "error")),
    ]
    if any(status == "error" for status in component_statuses):
        report["status"] = "error"
    elif any(status == "degraded" for status in component_statuses):
        report["status"] = "degraded"
    else:
        report["status"] = "ok"

    return report
