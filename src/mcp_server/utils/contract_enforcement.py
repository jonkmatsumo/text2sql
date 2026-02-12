"""MCP tool response contract enforcement at the registry boundary."""

from __future__ import annotations

import functools
import json
import logging
from typing import Any, Awaitable, Callable, ParamSpec, TypeVar

from common.models.error_metadata import ErrorCategory, ToolError
from common.models.tool_envelopes import (
    ExecuteSQLQueryResponseEnvelope,
    GenericToolMetadata,
    ToolResponseEnvelope,
)

logger = logging.getLogger(__name__)

P = ParamSpec("P")
R = TypeVar("R")


def _validate_envelope_payload(payload: Any) -> bool:
    if not isinstance(payload, dict):
        return False

    try:
        ExecuteSQLQueryResponseEnvelope.model_validate(payload)
        return True
    except Exception:
        pass

    try:
        ToolResponseEnvelope.model_validate(payload)
        return True
    except Exception:
        return False


def _to_payload(response: Any) -> tuple[Any, str, str | None]:
    if isinstance(response, str):
        try:
            return json.loads(response), "json_string", None
        except Exception as exc:
            return None, "raw_string", type(exc).__name__

    if isinstance(response, dict):
        return response, "dict", None

    if hasattr(response, "model_dump"):
        try:
            dumped = response.model_dump(exclude_none=True)
            return dumped, "model", None
        except Exception as exc:
            return None, "model", type(exc).__name__

    return None, type(response).__name__, None


def _build_malformed_envelope(
    *,
    tool_name: str,
    payload_kind: str,
    parse_error_type: str | None,
) -> str:
    error = ToolError(
        category=ErrorCategory.TOOL_RESPONSE_MALFORMED,
        code="TOOL_RESPONSE_MALFORMED",
        message="Tool response failed envelope contract validation.",
        retryable=False,
        provider=tool_name,
        details_safe={"tool_name": tool_name},
        details_debug={
            "payload_kind": payload_kind,
            "parse_error_type": parse_error_type,
        },
    )
    envelope = ToolResponseEnvelope(
        result=None,
        metadata=GenericToolMetadata(provider=tool_name),
        error=error,
    )
    return envelope.model_dump_json(exclude_none=True)


def enforce_tool_response_contract(
    tool_name: str,
) -> Callable[[Callable[P, Awaitable[R]]], Callable[P, Awaitable[R | str]]]:
    """Wrap a tool handler and enforce envelope shape on every response."""

    def decorator(func: Callable[P, Awaitable[R]]) -> Callable[P, Awaitable[R | str]]:
        @functools.wraps(func)
        async def wrapper(*args: P.args, **kwargs: P.kwargs) -> R | str:
            response = await func(*args, **kwargs)
            payload, payload_kind, parse_error_type = _to_payload(response)

            if payload is None or not _validate_envelope_payload(payload):
                logger.warning(
                    (
                        "Tool response contract violation for %s "
                        "(payload_kind=%s, parse_error_type=%s)"
                    ),
                    tool_name,
                    payload_kind,
                    parse_error_type,
                )
                return _build_malformed_envelope(
                    tool_name=tool_name,
                    payload_kind=payload_kind,
                    parse_error_type=parse_error_type,
                )

            return response

        return wrapper

    return decorator
