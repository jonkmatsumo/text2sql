"""Tests for per-run prompt byte budgeting helpers."""

from agent.utils.prompt_budget import consume_prompt_budget


def test_consume_prompt_budget_tracks_increment_and_limit(monkeypatch):
    """Budget consumption should track byte increments and trigger when over limit."""
    monkeypatch.setenv("AGENT_MAX_PROMPT_BYTES_PER_RUN", "2048")

    used, increment, exceeded, limit = consume_prompt_budget(0, {"a": "12345"})
    assert used == increment
    assert limit == 2048
    assert exceeded is False

    used2, increment2, exceeded2, limit2 = consume_prompt_budget(used, {"b": "x" * 3000})
    assert used2 == used + increment2
    assert limit2 == 2048
    assert exceeded2 is True
