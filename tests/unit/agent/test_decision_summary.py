"""Unit tests for deterministic agent decision/retry summaries."""

from agent.state.decision_summary import build_decision_summary, build_retry_correction_summary


def test_build_decision_summary_is_deterministic_and_bounded():
    """Decision summary should be deterministic and respect max table bounds."""
    state = {
        "table_names": ["Orders", "users", "orders"],
        "retry_count": 2,
        "schema_refresh_count": 1,
        "validation_failures": [
            {
                "rejected_tables": [
                    {"table": "payments", "reason": "table_not_allowlisted"},
                    {"table": "orders", "reason": "restricted_table"},
                ]
            }
        ],
        "ast_validation_result": {
            "metadata": {
                "join_count": 2,
                "estimated_table_count": 3,
                "estimated_scan_columns": 7,
                "union_count": 1,
                "detected_cartesian_flag": False,
                "query_complexity_score": 16,
            },
            "violations": [
                {"details": {"table": "inventory", "reason": "table_not_allowlisted"}},
                {
                    "details": {
                        "tables": ["archive.orders", "users"],
                        "reason": "set_operation_disallowed_table",
                    }
                },
            ],
        },
    }

    summary = build_decision_summary(state, max_tables=3)

    assert summary["selected_tables"] == ["orders", "users"]
    assert summary["rejected_tables"] == [
        {"table": "archive.orders", "reason": "set_operation_disallowed_table"},
        {"table": "inventory", "reason": "table_not_allowlisted"},
        {"table": "orders", "reason": "restricted_table"},
    ]
    assert summary["retry_count"] == 2
    assert summary["schema_refresh_events"] == 1
    assert summary["query_complexity"] == {
        "join_count": 2,
        "estimated_table_count": 3,
        "estimated_scan_columns": 7,
        "union_count": 1,
        "detected_cartesian_flag": False,
        "query_complexity_score": 16,
    }


def test_build_retry_correction_summary_counts_all_events_and_bounds_payloads():
    """Retry summary should count all events while bounding emitted detail payloads."""
    state = {
        "correction_attempts": [
            {"attempt": 1, "outcome": "corrected"},
            {"attempt": 2, "outcome": "terminal_stop"},
        ],
        "validation_failures": [
            {"retry_count": 0, "violation_types": ["restricted_table"]},
            {"retry_count": 1, "violation_types": ["column_allowlist"]},
        ],
        "retry_reason": "max_retries_reached",
    }

    summary = build_retry_correction_summary(state, max_events=1)

    assert summary["correction_attempt_count"] == 2
    assert summary["validation_failure_count"] == 2
    assert summary["correction_attempts"] == [{"attempt": 1, "outcome": "corrected"}]
    assert summary["validation_failures"] == [
        {"retry_count": 0, "violation_types": ["restricted_table"]}
    ]
    assert summary["final_stopping_reason"] == "max_retries_reached"


def test_build_retry_correction_summary_falls_back_to_termination_reason():
    """Stopping reason should fall back to explicit termination reason when present."""
    state = {
        "correction_attempts": [],
        "validation_failures": [],
        "termination_reason": "completed",
    }

    summary = build_retry_correction_summary(state, max_events=5)

    assert summary["final_stopping_reason"] == "completed"
