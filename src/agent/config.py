"""Agent configuration helpers."""

import logging

from common.config.env import get_env_float, get_env_str

logger = logging.getLogger(__name__)

_DEFAULT_SYNTHESIZE_TEMPERATURE = 0.7

# Semantic version of the agent's system prompts.
# Increment this when changing prompt templates in nodes.
PROMPT_VERSION = "1.0.0"


def get_synthesize_temperature() -> float:
    """Return synthesize temperature with deterministic override support."""
    mode = (get_env_str("AGENT_SYNTHESIZE_MODE", "") or "").strip().lower()
    if mode == "deterministic":
        return 0.0

    try:
        value = get_env_float("AGENT_SYNTHESIZE_TEMPERATURE", None)
    except ValueError as exc:
        logger.warning("Invalid AGENT_SYNTHESIZE_TEMPERATURE: %s", exc)
        return _DEFAULT_SYNTHESIZE_TEMPERATURE

    if value is None:
        return _DEFAULT_SYNTHESIZE_TEMPERATURE
    return max(0.0, float(value))
