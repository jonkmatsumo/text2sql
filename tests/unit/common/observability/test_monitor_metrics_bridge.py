"""Tests for in-memory agent monitor OTEL metric bridging."""

from unittest.mock import patch

from common.observability.monitor import AgentMonitor, RunSummary


def test_agent_monitor_record_run_emits_counter_and_histogram():
    """Recording a run should export low-cardinality monitor metrics."""
    monitor = AgentMonitor(max_history=5)
    summary = RunSummary(
        run_id="run-1",
        timestamp=1700000000.0,
        status="success",
        error_category=None,
        duration_ms=123.4,
        tenant_id=1,
        llm_calls=2,
        llm_tokens=50,
    )

    with (
        patch("common.observability.monitor.agent_metrics.add_counter") as mock_add_counter,
        patch("common.observability.monitor.agent_metrics.record_histogram") as mock_histogram,
    ):
        monitor.record_run(summary)

    counter_names = [call.args[0] for call in mock_add_counter.call_args_list]
    assert "agent.monitor.run_total" in counter_names
    assert "agent.monitor.requests_total" in counter_names
    mock_histogram.assert_called_once()


def test_agent_monitor_records_budget_exhaustion_counter():
    """Budget-exhausted runs should increment dedicated monitor metrics."""
    monitor = AgentMonitor(max_history=5)
    summary = RunSummary(
        run_id="run-2",
        timestamp=1700000001.0,
        status="error",
        error_category="budget_exceeded",
        duration_ms=45.0,
        tenant_id=1,
        llm_calls=1,
        llm_tokens=120,
    )

    with patch("common.observability.monitor.agent_metrics.add_counter") as mock_add_counter:
        monitor.record_run(summary)

    counter_names = [call.args[0] for call in mock_add_counter.call_args_list]
    assert "agent.monitor.token_budget_exhausted_total" in counter_names
    snapshot = monitor.get_snapshot()
    assert snapshot["counters"]["token_budget_exhausted"] == 1


def test_agent_monitor_increment_emits_event_counter():
    """Incrementing tracked counters should export monitor event metrics."""
    monitor = AgentMonitor(max_history=5)

    with patch("common.observability.monitor.agent_metrics.add_counter") as mock_add_counter:
        monitor.increment("circuit_breaker_open")

    mock_add_counter.assert_called_once()
