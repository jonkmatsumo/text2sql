import threading
from collections import deque
from dataclasses import asdict, dataclass
from typing import Any, Dict, Optional

from common.observability.metrics import agent_metrics


@dataclass
class RunSummary:
    """Summary of a single agent execution."""

    run_id: str
    timestamp: float
    status: str
    error_category: Optional[str]
    duration_ms: float
    tenant_id: int
    llm_calls: int
    llm_tokens: int


class AgentMonitor:
    """In-memory monitor for agent operational metrics."""

    def __init__(self, max_history: int = 50):
        """Initialize the monitor with a fixed-size history buffer."""
        self.max_history = max_history
        self.run_history: deque[RunSummary] = deque(maxlen=max_history)
        self.counters: Dict[str, int] = {
            "request_total": 0,
            "request_succeeded": 0,
            "request_failed": 0,
            "token_budget_exhausted": 0,
            "circuit_breaker_open": 0,
            "rate_limited": 0,
            "tenant_limit_exceeded": 0,
            "schema_refresh_storm": 0,
        }
        self._lock = threading.Lock()

    def record_run(self, summary: RunSummary) -> None:
        """Record a completed run summary."""
        status = str(summary.status or "").strip().lower()
        request_outcome = "succeeded" if status == "success" else "failed"
        error_category = str(summary.error_category or "").strip().lower()
        budget_exhausted = error_category in {"budget_exhausted", "budget_exceeded"}

        with self._lock:
            self.run_history.append(summary)
            self.counters["request_total"] += 1
            if request_outcome == "succeeded":
                self.counters["request_succeeded"] += 1
            else:
                self.counters["request_failed"] += 1
            if budget_exhausted:
                self.counters["token_budget_exhausted"] += 1
        agent_metrics.add_counter(
            "agent.monitor.run_total",
            description="Count of completed agent runs",
            attributes={"status": summary.status},
        )
        agent_metrics.add_counter(
            "agent.monitor.requests_total",
            description="Count of completed agent requests by final outcome",
            attributes={"outcome": request_outcome},
        )
        if budget_exhausted:
            agent_metrics.add_counter(
                "agent.monitor.token_budget_exhausted_total",
                description="Count of agent runs ending due to token budget exhaustion",
            )
        agent_metrics.record_histogram(
            "agent.monitor.run.duration_ms",
            float(summary.duration_ms),
            unit="ms",
            description="Duration of completed agent runs in milliseconds",
            attributes={"status": summary.status},
        )

    def increment(self, counter: str) -> None:
        """Increment a named counter safely."""
        with self._lock:
            if counter in self.counters:
                self.counters[counter] += 1
                agent_metrics.add_counter(
                    "agent.monitor.event_total",
                    description="Count of agent monitor lifecycle events",
                    attributes={"counter": counter},
                )

    def get_snapshot(self) -> Dict[str, Any]:
        """Return a snapshot of current metrics and history."""
        with self._lock:
            return {
                "recent_runs": [asdict(r) for r in reversed(list(self.run_history))],
                "counters": self.counters.copy(),
            }


# Global singleton
agent_monitor = AgentMonitor()
