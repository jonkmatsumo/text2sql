"""Per-run LLM token budget tracking and enforcement."""

from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from typing import Any, Iterator


@dataclass
class LLMBudgetState:
    """Mutable run-scoped counters for approximate LLM token usage."""

    max_tokens: int
    prompt_total: int = 0
    completion_total: int = 0

    @property
    def total(self) -> int:
        """Total prompt + completion tokens consumed in this run."""
        return int(self.prompt_total + self.completion_total)

    @property
    def remaining(self) -> int:
        """Remaining estimated token budget for this run."""
        return max(0, int(self.max_tokens - self.total))


class LLMBudgetExceededError(RuntimeError):
    """Raised when a run-level LLM budget would be exceeded by another call."""

    def __init__(self, state: LLMBudgetState, requested_tokens: int):
        """Create a typed budget exception with safe telemetry context."""
        super().__init__("LLM budget exceeded")
        self.state = state
        self.requested_tokens = max(0, int(requested_tokens))


_RUN_LLM_BUDGET: ContextVar[LLMBudgetState | None] = ContextVar("run_llm_budget", default=None)


def estimate_token_count(value: Any) -> int:
    """Approximate token count from serialized text size."""
    if value is None:
        return 0
    if isinstance(value, str):
        text = value
    else:
        text = str(value)
    if not text:
        return 0
    # Rough heuristic: ~4 chars/token for English-like prompts.
    return max(1, (len(text) + 3) // 4)


def current_budget_state() -> LLMBudgetState | None:
    """Return the active run-level LLM budget state, if configured."""
    return _RUN_LLM_BUDGET.get()


@contextmanager
def llm_run_budget_context(max_tokens: int | None) -> Iterator[None]:
    """Install a run-scoped LLM budget for all nested LLM client calls."""
    if max_tokens is None or int(max_tokens) <= 0:
        yield
        return

    token = _RUN_LLM_BUDGET.set(LLMBudgetState(max_tokens=max(1, int(max_tokens))))
    try:
        yield
    finally:
        _RUN_LLM_BUDGET.reset(token)


def consume_prompt_tokens(estimated_tokens: int) -> LLMBudgetState | None:
    """Reserve prompt tokens for the next LLM call or raise on budget overflow."""
    state = _RUN_LLM_BUDGET.get()
    if state is None:
        return None

    requested = max(0, int(estimated_tokens))
    if state.total + requested > state.max_tokens:
        raise LLMBudgetExceededError(state=state, requested_tokens=requested)

    state.prompt_total += requested
    return state


def reconcile_prompt_tokens(
    *,
    estimated_tokens: int,
    actual_tokens: int | None,
) -> LLMBudgetState | None:
    """Adjust prompt totals when provider-reported usage is available."""
    state = _RUN_LLM_BUDGET.get()
    if state is None or actual_tokens is None:
        return state

    delta = int(actual_tokens) - max(0, int(estimated_tokens))
    if delta > 0 and state.total + delta > state.max_tokens:
        raise LLMBudgetExceededError(state=state, requested_tokens=delta)
    state.prompt_total = max(0, int(state.prompt_total + delta))
    return state


def consume_completion_tokens(estimated_tokens: int) -> LLMBudgetState | None:
    """Reserve completion tokens after a call or raise if budget is exhausted."""
    state = _RUN_LLM_BUDGET.get()
    if state is None:
        return None

    requested = max(0, int(estimated_tokens))
    if state.total + requested > state.max_tokens:
        raise LLMBudgetExceededError(state=state, requested_tokens=requested)

    state.completion_total += requested
    return state


def budget_telemetry_attributes(state: LLMBudgetState | None) -> dict[str, int]:
    """Build stable telemetry attributes for run-level budget usage."""
    if state is None:
        return {}
    return {
        "llm.tokens.prompt_total": int(state.prompt_total),
        "llm.tokens.completion_total": int(state.completion_total),
        "llm.budget.remaining_estimate": int(state.remaining),
    }
