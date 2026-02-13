import threading
from collections import deque
from dataclasses import asdict, dataclass
from typing import Any, Dict, Optional


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
            "circuit_breaker_open": 0,
            "rate_limited": 0,
            "tenant_limit_exceeded": 0,
            "schema_refresh_storm": 0,
        }
        self._lock = threading.Lock()

    def record_run(self, summary: RunSummary) -> None:
        """Record a completed run summary."""
        with self._lock:
            self.run_history.append(summary)

    def increment(self, counter: str) -> None:
        """Increment a named counter safely."""
        with self._lock:
            if counter in self.counters:
                self.counters[counter] += 1

    def get_snapshot(self) -> Dict[str, Any]:
        """Return a snapshot of current metrics and history."""
        with self._lock:
            return {
                "recent_runs": [asdict(r) for r in reversed(list(self.run_history))],
                "counters": self.counters.copy(),
            }


# Global singleton
agent_monitor = AgentMonitor()
