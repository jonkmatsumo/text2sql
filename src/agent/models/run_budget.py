"""Run-level budget partitioning models and helpers."""

from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from typing import Iterator, Optional


@dataclass
class RunBudget:
    """Structured per-run budget partition for LLM, tools, rows, and time."""

    llm_token_budget: int
    tool_call_budget: int
    sql_row_budget: int
    time_budget_ms: int
    tool_calls_total: int = 0
    rows_total: int = 0
    tool_call_budget_exceeded: bool = False
    sql_row_budget_exceeded: bool = False

    def to_state_dict(self) -> dict[str, int | bool]:
        """Serialize budget state for LangGraph state persistence."""
        return {
            "llm_token_budget": max(0, int(self.llm_token_budget)),
            "tool_call_budget": max(0, int(self.tool_call_budget)),
            "sql_row_budget": max(0, int(self.sql_row_budget)),
            "time_budget_ms": max(0, int(self.time_budget_ms)),
            "tool_calls_total": max(0, int(self.tool_calls_total)),
            "rows_total": max(0, int(self.rows_total)),
            "tool_call_budget_exceeded": bool(self.tool_call_budget_exceeded),
            "sql_row_budget_exceeded": bool(self.sql_row_budget_exceeded),
        }

    @classmethod
    def from_state_dict(cls, payload: Optional[dict]) -> Optional["RunBudget"]:
        """Deserialize budget state from a dictionary payload."""
        if not isinstance(payload, dict):
            return None
        return cls(
            llm_token_budget=max(0, int(payload.get("llm_token_budget", 0) or 0)),
            tool_call_budget=max(0, int(payload.get("tool_call_budget", 0) or 0)),
            sql_row_budget=max(0, int(payload.get("sql_row_budget", 0) or 0)),
            time_budget_ms=max(0, int(payload.get("time_budget_ms", 0) or 0)),
            tool_calls_total=max(0, int(payload.get("tool_calls_total", 0) or 0)),
            rows_total=max(0, int(payload.get("rows_total", 0) or 0)),
            tool_call_budget_exceeded=bool(payload.get("tool_call_budget_exceeded", False)),
            sql_row_budget_exceeded=bool(payload.get("sql_row_budget_exceeded", False)),
        )


class RunBudgetExceededError(RuntimeError):
    """Raised when a run-level budget partition is exceeded."""

    category = "budget_exceeded"
    code = "BUDGET_EXCEEDED"

    def __init__(self, *, dimension: str, limit: int, used: int, requested: int) -> None:
        """Capture structured budget overflow metadata."""
        self.dimension = str(dimension)
        self.limit = max(0, int(limit))
        self.used = max(0, int(used))
        self.requested = max(0, int(requested))
        super().__init__("Run budget exceeded.")


_RUN_BUDGET: ContextVar[RunBudget | None] = ContextVar("run_budget", default=None)


def current_run_budget() -> RunBudget | None:
    """Return the currently active run budget context, if present."""
    return _RUN_BUDGET.get()


@contextmanager
def run_budget_context(budget: RunBudget | None) -> Iterator[None]:
    """Install a run-scoped budget state for nested tool/execute calls."""
    if budget is None:
        yield
        return
    token = _RUN_BUDGET.set(budget)
    try:
        yield
    finally:
        _RUN_BUDGET.reset(token)


def consume_tool_call_budget(call_count: int = 1) -> RunBudget | None:
    """Reserve tool-call budget or raise a typed budget-exceeded error."""
    budget = _RUN_BUDGET.get()
    if budget is None:
        return None
    requested = max(0, int(call_count))
    projected = int(budget.tool_calls_total) + requested
    if projected > int(budget.tool_call_budget):
        budget.tool_call_budget_exceeded = True
        raise RunBudgetExceededError(
            dimension="tool_calls",
            limit=int(budget.tool_call_budget),
            used=int(budget.tool_calls_total),
            requested=requested,
        )
    budget.tool_calls_total = projected
    return budget


def consume_sql_row_budget(row_count: int) -> RunBudget | None:
    """Reserve SQL row budget or raise a typed budget-exceeded error."""
    budget = _RUN_BUDGET.get()
    if budget is None:
        return None
    requested = max(0, int(row_count))
    projected = int(budget.rows_total) + requested
    if projected > int(budget.sql_row_budget):
        budget.sql_row_budget_exceeded = True
        raise RunBudgetExceededError(
            dimension="sql_rows",
            limit=int(budget.sql_row_budget),
            used=int(budget.rows_total),
            requested=requested,
        )
    budget.rows_total = projected
    return budget
