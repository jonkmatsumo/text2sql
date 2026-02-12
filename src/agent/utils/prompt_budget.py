"""Per-run LLM prompt budget helpers."""

from __future__ import annotations

import json
from typing import Any

from common.config.env import get_env_int

DEFAULT_MAX_PROMPT_BYTES_PER_RUN = 8 * 1024 * 1024


def max_prompt_bytes_per_run() -> int:
    """Return configured max prompt bytes per run with sane lower bound."""
    try:
        configured = get_env_int(
            "AGENT_MAX_PROMPT_BYTES_PER_RUN",
            DEFAULT_MAX_PROMPT_BYTES_PER_RUN,
        )
    except ValueError:
        configured = DEFAULT_MAX_PROMPT_BYTES_PER_RUN
    if configured is None:
        configured = DEFAULT_MAX_PROMPT_BYTES_PER_RUN
    return max(1024, int(configured))


def estimate_prompt_bytes(payload: Any) -> int:
    """Estimate prompt bytes from a payload in a deterministic way."""
    try:
        serialized = json.dumps(payload, default=str, separators=(",", ":"))
    except Exception:
        serialized = str(payload)
    return len(serialized.encode("utf-8"))


def consume_prompt_budget(
    used_so_far: int,
    payload: Any,
) -> tuple[int, int, bool, int]:
    """Consume prompt bytes and return (new_total, increment, exceeded, limit)."""
    increment = estimate_prompt_bytes(payload)
    limit = max_prompt_bytes_per_run()
    new_total = max(0, int(used_so_far)) + int(increment)
    exceeded = new_total > limit
    return new_total, increment, exceeded, limit
