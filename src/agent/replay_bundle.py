"""Deterministic replay bundle capture and validation utilities."""

import json
from datetime import datetime, timezone
from typing import Any, Optional

from pydantic import BaseModel, Field, ValidationError

from common.config.env import get_env_int, get_env_str
from common.sanitization.text import redact_sensitive_info

REPLAY_BUNDLE_VERSION = "1.0"
MAX_TOOL_ROWS = 50


class ReplayToolIO(BaseModel):
    """Captured tool invocation summary for replay/debug."""

    name: str
    input: dict[str, Any]
    output: dict[str, Any]


class ReplayBundle(BaseModel):
    """Replay bundle schema."""

    version: str = Field(default=REPLAY_BUNDLE_VERSION)
    captured_at: str
    model: dict[str, Any]
    seed: Optional[int] = None
    prompts: dict[str, Any]
    schema_context: dict[str, Any]
    flags: dict[str, Any]
    tool_io: list[ReplayToolIO]
    outcome: dict[str, Any]


def _redact_recursive(value: Any) -> Any:
    if isinstance(value, str):
        return redact_sensitive_info(value)
    if isinstance(value, list):
        return [_redact_recursive(item) for item in value]
    if isinstance(value, dict):
        redacted: dict[str, Any] = {}
        for key, item in value.items():
            lowered = key.lower()
            if any(
                token in lowered for token in ("token", "password", "secret", "api_key", "auth")
            ):
                redacted[key] = "<redacted>"
            else:
                redacted[key] = _redact_recursive(item)
        return redacted
    return value


def _bounded_rows(rows: Any, max_rows: int = MAX_TOOL_ROWS) -> list[dict[str, Any]]:
    if not isinstance(rows, list):
        return []
    bounded = rows[:max_rows]
    return [_redact_recursive(row) for row in bounded if isinstance(row, dict)]


def collect_replay_flags() -> dict[str, Any]:
    """Collect relevant runtime flags for reproducibility."""
    keys = [
        "AGENT_REPLAY_MODE",
        "AGENT_RETRY_POLICY",
        "AGENT_MAX_RETRIES",
        "AGENT_CAPABILITY_FALLBACK_MODE",
        "AGENT_PROVIDER_CAP_MITIGATION",
        "AGENT_AUTO_PAGINATION",
        "AGENT_AUTO_PAGINATION_MAX_PAGES",
        "AGENT_AUTO_PAGINATION_MAX_ROWS",
        "AGENT_PREFETCH_NEXT_PAGE",
        "AGENT_PREFETCH_MAX_CONCURRENCY",
        "QUERY_TARGET_BACKEND",
    ]
    collected: dict[str, Any] = {}
    for key in keys:
        value = get_env_str(key)
        if value is not None:
            collected[key] = value
    return collected


def _resolve_seed(state: dict[str, Any]) -> Optional[int]:
    seed = state.get("seed")
    if isinstance(seed, int):
        return seed
    try:
        return get_env_int("AGENT_SEED", None)
    except ValueError:
        return None


def build_replay_bundle(
    *,
    question: str,
    state: dict[str, Any],
    request_payload: dict[str, Any],
) -> ReplayBundle:
    """Build a validated replay bundle from runtime state."""
    response_text = None
    if state.get("messages"):
        last_message = state["messages"][-1]
        response_text = getattr(last_message, "content", str(last_message))

    tool_io: list[ReplayToolIO] = []
    if state.get("last_tool_output"):
        # If we have the raw last tool output, capture it (bounded)
        last_output = state["last_tool_output"]
        if isinstance(last_output, dict) and "rows" in last_output:
            # Create a bounded version of the raw output
            bounded_output = last_output.copy()
            bounded_output["rows"] = _bounded_rows(last_output.get("rows"))

            tool_io.append(
                ReplayToolIO(
                    name="execute_sql_query",
                    input=_redact_recursive(
                        {
                            "sql_query": state.get("current_sql"),
                            "tenant_id": request_payload.get("tenant_id"),
                            "page_token": request_payload.get("page_token"),
                            "page_size": request_payload.get("page_size"),
                        }
                    ),
                    output=_redact_recursive(bounded_output),
                )
            )
    elif state.get("current_sql"):
        # Fallback to summary if raw output not available
        tool_io.append(
            ReplayToolIO(
                name="execute_sql_query",
                input=_redact_recursive(
                    {
                        "sql_query": state.get("current_sql"),
                        "tenant_id": request_payload.get("tenant_id"),
                        "page_token": request_payload.get("page_token"),
                        "page_size": request_payload.get("page_size"),
                    }
                ),
                output=_redact_recursive(
                    {
                        "rows": _bounded_rows(state.get("query_result")),
                        "metadata": {
                            "rows_returned": len(state.get("query_result") or []),
                            "error": state.get("error"),
                            "error_category": state.get("error_category"),
                        },
                        "response_shape": "enveloped",
                    }
                ),
            )
        )

    bundle = ReplayBundle(
        captured_at=datetime.now(timezone.utc).isoformat(),
        model={
            "provider": get_env_str("LLM_PROVIDER", "openai"),
            "model_id": get_env_str("LLM_MODEL", "gpt-4o"),
            "temperature": get_env_str("LLM_TEMPERATURE"),
        },
        seed=_resolve_seed(state),
        prompts=_redact_recursive(
            {
                "user": question,
                "assistant": response_text,
            }
        ),
        schema_context={
            "schema_snapshot_id": state.get("schema_snapshot_id"),
            "fingerprint": state.get("schema_snapshot_id"),
        },
        flags=collect_replay_flags(),
        tool_io=tool_io,
        outcome=_redact_recursive(
            {
                "sql": state.get("current_sql"),
                "result": _bounded_rows(state.get("query_result")),
                "response": response_text,
                "error": state.get("error"),
                "error_category": state.get("error_category"),
                "retry_summary": state.get("retry_summary"),
                "result_completeness": state.get("result_completeness"),
            }
        ),
    )
    return bundle


def serialize_replay_bundle(bundle: ReplayBundle) -> str:
    """Serialize replay bundle deterministically."""
    payload = bundle.model_dump(mode="json")
    return json.dumps(payload, sort_keys=True, separators=(",", ":"))


def validate_replay_bundle(bundle_payload: dict[str, Any]) -> ReplayBundle:
    """Validate replay bundle payload using schema model."""
    return ReplayBundle.model_validate(bundle_payload)


def replay_response_from_bundle(
    bundle: ReplayBundle, allow_external_calls: bool = False
) -> dict[str, Any]:
    """Build a replay response envelope from captured bundle content."""
    if allow_external_calls:
        return {
            "mode": "external_calls_requested",
            "note": "Replay bundle accepted; external execution is caller-managed.",
        }

    outcome = bundle.outcome
    return {
        "current_sql": outcome.get("sql"),
        "query_result": outcome.get("result"),
        "error": outcome.get("error"),
        "error_category": outcome.get("error_category"),
        "retry_summary": outcome.get("retry_summary"),
        "result_completeness": outcome.get("result_completeness"),
        "messages": [type("ReplayMessage", (), {"content": outcome.get("response") or ""})()],
    }


def lookup_replay_tool_output(
    bundle_data: Optional[dict[str, Any]],
    tool_name: str,
    tool_input: dict[str, Any],
) -> Optional[dict[str, Any]]:
    """Look up a captured tool output in the replay bundle by name and input fingerprint."""
    if not bundle_data:
        return None

    tool_io = bundle_data.get("tool_io", [])
    if not tool_io:
        return None

    # Simple match: same tool name.
    # We could refine this by fingerprinting the input, but for execute_sql_query
    # we usually only have one main call per turn or they are sequential.
    for io in tool_io:
        io_name = io.get("name")
        if io_name == tool_name:
            # If it's execute_sql_query, we might want to check the SQL
            if tool_name == "execute_sql_query":
                io_input = io.get("input", {})
                if io_input.get("sql_query") == tool_input.get("sql_query"):
                    return io.get("output")
            else:
                return io.get("output")

    return None


__all__ = [
    "ReplayBundle",
    "ReplayToolIO",
    "REPLAY_BUNDLE_VERSION",
    "build_replay_bundle",
    "serialize_replay_bundle",
    "validate_replay_bundle",
    "replay_response_from_bundle",
    "ValidationError",
]
