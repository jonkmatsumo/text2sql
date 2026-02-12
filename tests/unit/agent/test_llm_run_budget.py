"""Tests for run-level LLM token budget enforcement."""

from __future__ import annotations

import pytest
from langchain_core.messages import AIMessage

from agent.graph import route_after_validation
from agent.llm_client import _wrap_llm
from agent.utils.llm_run_budget import LLMBudgetExceededError, llm_run_budget_context


class _FakeLLM:
    """Minimal fake LLM with deterministic token usage metadata."""

    model_name = "fake-llm"

    def invoke(self, input_data, config=None, **kwargs):  # noqa: ARG002
        return AIMessage(
            content="SELECT 1",
            response_metadata={
                "token_usage": {
                    "prompt_tokens": 4,
                    "completion_tokens": 12,
                    "total_tokens": 16,
                }
            },
        )

    async def ainvoke(self, input_data, config=None, **kwargs):  # noqa: ARG002
        return self.invoke(input_data, config=config, **kwargs)


def test_llm_wrapper_raises_budget_exceeded_when_run_budget_is_spent():
    """Wrapper should stop further LLM calls once run-level token budget is exceeded."""
    wrapped = _wrap_llm(_FakeLLM())

    with llm_run_budget_context(8):
        with pytest.raises(LLMBudgetExceededError):
            wrapped.invoke("select one")


def test_route_after_validation_stops_retry_on_budget_exceeded():
    """Budget-exceeded generation/validation state should short-circuit to synthesis."""
    state = {
        "retry_count": 0,
        "ast_validation_result": None,
        "error": "LLM token budget exceeded for this request.",
        "error_category": "budget_exceeded",
    }
    assert route_after_validation(state) == "synthesize"
