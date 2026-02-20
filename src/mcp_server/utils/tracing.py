"""Tracing wrapper for MCP tools.

Tool-version negotiation semantics:
- Missing `requested_tool_version`: execute with current supported version.
- Well-formed but unsupported version: return compatibility error envelope.
- Malformed version string: return deterministic validation error envelope.
"""

import functools
import json
import logging
import re
import time
from typing import Any, Awaitable, Callable, ParamSpec, TypeVar

from opentelemetry import propagate, trace
from opentelemetry.trace import Status, StatusCode

from common.config.env import get_env_str
from common.constants.reason_codes import ToolVersionNegotiationErrorCode
from common.errors.error_codes import ErrorCode, canonical_error_code_for_category
from common.models.error_metadata import ErrorCategory
from common.models.tool_versions import get_tool_version
from common.observability.metrics import mcp_metrics
from common.tenancy.limits import TenantConcurrencyLimitExceeded, get_mcp_tool_tenant_limiter
from mcp_server.utils.errors import tool_error_response
from mcp_server.utils.reserved_fields import (
    REQUEST_ID_RESERVED_FIELD,
    TRACE_CONTEXT_RESERVED_FIELD,
    split_reserved_tool_metadata,
)

logger = logging.getLogger(__name__)

P = ParamSpec("P")
R = TypeVar("R")
_TOOL_VERSION_PATTERN = re.compile(r"^v[1-9]\d*$")


def _coerce_bool(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"true", "false"}:
            return lowered == "true"
    return None


def _read_is_truncated(payload: dict[str, Any]) -> bool:
    direct = _coerce_bool(payload.get("is_truncated"))
    if direct is not None:
        return direct

    metadata = payload.get("metadata")
    if isinstance(metadata, dict):
        nested = _coerce_bool(metadata.get("is_truncated"))
        if nested is not None:
            return nested
        fallback = _coerce_bool(metadata.get("truncated"))
        if fallback is not None:
            return fallback

    return False


def _extract_truncation_signal(response: Any) -> tuple[bool, bool]:
    """Return (is_truncated, parse_failed) from tool response payload."""
    if isinstance(response, dict):
        return _read_is_truncated(response), False

    if hasattr(response, "model_dump"):
        try:
            dumped = response.model_dump()
            if isinstance(dumped, dict):
                return _read_is_truncated(dumped), False
        except Exception:
            return False, True

    if isinstance(response, str):
        try:
            payload = json.loads(response)
        except json.JSONDecodeError:
            return False, True
        except Exception:
            return False, True
        if isinstance(payload, dict):
            return _read_is_truncated(payload), False
        return False, False

    return False, False


def _extract_trace_context(payload: Any) -> dict[str, str]:
    """Extract W3C trace carrier values from reserved tool kwargs."""
    if not isinstance(payload, dict):
        return {}

    carrier: dict[str, str] = {}
    traceparent = payload.get("traceparent")
    tracestate = payload.get("tracestate")

    if isinstance(traceparent, str) and traceparent.strip():
        carrier["traceparent"] = traceparent.strip()
    if isinstance(tracestate, str) and tracestate.strip():
        carrier["tracestate"] = tracestate.strip()

    return carrier


def _normalize_request_id(value: Any) -> str | None:
    """Normalize request identifier values from reserved args/context."""
    if value is None:
        return None
    normalized = str(value).strip()
    return normalized or None


def _current_trace_id(span: Any) -> str | None:
    """Return active span trace_id as canonical hex string, when available."""
    try:
        span_context = span.get_span_context()
    except Exception:
        return None
    if span_context is None or not getattr(span_context, "is_valid", False):
        return None
    trace_id = getattr(span_context, "trace_id", 0)
    if not isinstance(trace_id, int) or trace_id <= 0:
        return None
    return format(trace_id, "032x")


def _extract_envelope_payload(response: Any) -> dict[str, Any] | None:
    """Extract dictionary payload from typed/enveloped tool outputs."""
    payload: dict[str, Any] | None = None
    if isinstance(response, dict):
        payload = response
    elif hasattr(response, "model_dump"):
        try:
            dumped = response.model_dump()
            if isinstance(dumped, dict):
                payload = dumped
        except Exception:
            payload = None
    elif isinstance(response, str):
        try:
            parsed = json.loads(response)
            if isinstance(parsed, dict):
                payload = parsed
        except Exception:
            payload = None
    return payload


def _extract_envelope_error_category(response: Any) -> str | None:
    """Extract envelope-level error category from response payload, if present."""
    payload = _extract_envelope_payload(response)

    if not isinstance(payload, dict):
        return None

    if "error" not in payload:
        return None

    error_payload = payload.get("error")
    if error_payload is None:
        return None

    if isinstance(error_payload, dict):
        category = error_payload.get("category")
        return str(category) if category is not None else "unknown"

    category_attr = getattr(error_payload, "category", None)
    if category_attr is not None:
        return str(getattr(category_attr, "value", category_attr))

    return "unknown"


def _extract_envelope_error_code(response: Any) -> str | None:
    """Extract canonical envelope-level error code from response payload, if present."""
    payload = _extract_envelope_payload(response)
    if not isinstance(payload, dict):
        return None
    if "error" not in payload:
        return None

    error_payload = payload.get("error")
    if error_payload is None:
        return None

    if isinstance(error_payload, dict):
        if error_payload.get("error_code") is not None:
            return str(error_payload.get("error_code"))
        category = error_payload.get("category")
        if category is not None:
            return canonical_error_code_for_category(str(category)).value
        if error_payload.get("code") is not None:
            return str(error_payload.get("code"))
        return ErrorCode.INTERNAL_ERROR.value

    error_code_attr = getattr(error_payload, "error_code", None)
    if error_code_attr is not None:
        return str(error_code_attr)

    category_attr = getattr(error_payload, "category", None)
    if category_attr is not None:
        normalized_category = str(getattr(category_attr, "value", category_attr))
        return canonical_error_code_for_category(normalized_category).value

    code_attr = getattr(error_payload, "code", None)
    if code_attr is not None:
        return str(code_attr)

    return ErrorCode.INTERNAL_ERROR.value


def _inject_tool_version(response: Any, tool_name: str) -> Any:
    """Inject tool_version into envelope metadata using central registry."""
    tool_version = get_tool_version(tool_name)

    def _apply_to_payload(payload: dict[str, Any]) -> dict[str, Any]:
        metadata = payload.get("metadata")
        if not isinstance(metadata, dict):
            metadata = {}
            payload["metadata"] = metadata
        metadata["tool_version"] = tool_version
        return payload

    if isinstance(response, dict):
        return _apply_to_payload(dict(response))

    if isinstance(response, str):
        try:
            payload = json.loads(response)
        except Exception:
            return response
        if not isinstance(payload, dict):
            return response
        payload = _apply_to_payload(payload)
        return json.dumps(payload, separators=(",", ":"))

    return response


def _inject_metadata_field(response: Any, *, field_name: str, field_value: str | None) -> Any:
    """Inject a metadata field into common envelope payload shapes."""
    if not field_name or not field_value:
        return response

    def _apply_to_payload(payload: dict[str, Any]) -> dict[str, Any]:
        metadata = payload.get("metadata")
        if not isinstance(metadata, dict):
            metadata = {}
            payload["metadata"] = metadata
        metadata[field_name] = field_value
        return payload

    if isinstance(response, dict):
        return _apply_to_payload(dict(response))

    if isinstance(response, str):
        try:
            payload = json.loads(response)
        except Exception:
            return response
        if not isinstance(payload, dict):
            return response
        payload = _apply_to_payload(payload)
        return json.dumps(payload, separators=(",", ":"))

    if hasattr(response, "model_dump"):
        try:
            payload = response.model_dump()
        except Exception:
            return response
        if not isinstance(payload, dict):
            return response
        payload = _apply_to_payload(payload)
        model_validate = getattr(type(response), "model_validate", None)
        if callable(model_validate):
            try:
                return model_validate(payload)
            except Exception:
                return payload
        return payload

    return response


def _inject_request_id(response: Any, request_id: str | None) -> Any:
    """Inject request_id into envelope metadata for cross-layer correlation."""
    return _inject_metadata_field(response, field_name="request_id", field_value=request_id)


def _inject_trace_id(response: Any, trace_id: str | None) -> Any:
    """Inject trace_id into envelope metadata for cross-layer correlation."""
    return _inject_metadata_field(response, field_name="trace_id", field_value=trace_id)


def _normalize_requested_tool_version(value: Any) -> tuple[str | None, str | None]:
    """Normalize and validate requested tool version input.

    Returns:
        (normalized_version, error_code)
        - If no version is requested, returns (None, None).
        - If malformed, returns (None, INVALID_TOOL_VERSION_REQUEST).
    """
    if value is None:
        return None, None
    normalized = str(value).strip()
    if not normalized:
        return None, ToolVersionNegotiationErrorCode.INVALID_TOOL_VERSION_REQUEST.value
    if not _TOOL_VERSION_PATTERN.fullmatch(normalized):
        return None, ToolVersionNegotiationErrorCode.INVALID_TOOL_VERSION_REQUEST.value
    return normalized, None


def trace_tool(tool_name: str) -> Callable[[Callable[P, Awaitable[R]]], Callable[P, Awaitable[R]]]:
    """Add OpenTelemetry tracing to an MCP tool handler.

    Args:
        tool_name: The name of the tool (e.g. "execute_sql_query").
    """

    def decorator(func: Callable[P, Awaitable[R]]) -> Callable[P, Awaitable[R]]:
        @functools.wraps(func)
        async def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            # Check enforcement mode
            mode = get_env_str("TELEMETRY_ENFORCEMENT_MODE", "warn").lower()
            tool_kwargs, reserved_kwargs = split_reserved_tool_metadata(kwargs)

            # Get a tracer
            tracer = trace.get_tracer("mcp.server")
            trace_context_payload = reserved_kwargs.get(TRACE_CONTEXT_RESERVED_FIELD)
            request_id = _normalize_request_id(reserved_kwargs.get(REQUEST_ID_RESERVED_FIELD))
            if request_id is None:
                from common.observability.context import request_id_var, run_id_var

                request_id = _normalize_request_id(request_id_var.get() or run_id_var.get())
            parent_context = None
            trace_carrier = _extract_trace_context(trace_context_payload)
            if trace_carrier:
                parent_context = propagate.extract(trace_carrier)

            # Check if tracing is actually enabled/configured
            # This is a heuristic: if we get a NoOpSpan, maybe tracing is off.
            # But start_as_current_span returns a Span.
            # We can check if the tracer provider is NoOp?
            # Or just assume if we are here we want to trace.

            span_name = f"mcp.tool.{tool_name}"

            with tracer.start_as_current_span(
                span_name,
                kind=trace.SpanKind.SERVER,
                context=parent_context,
            ) as span:
                trace_id = _current_trace_id(span)
                requested_tool_version = tool_kwargs.pop("requested_tool_version", None)
                supported_tool_version = get_tool_version(tool_name)
                normalized_version, version_parse_error = _normalize_requested_tool_version(
                    requested_tool_version
                )

                # 1. Record basic attributes
                span.set_attribute("mcp.tool.name", tool_name)
                span.set_attribute("mcp.tool.supported_version", supported_tool_version)
                if requested_tool_version is not None:
                    span.set_attribute("mcp.tool.requested_version", str(requested_tool_version))
                if request_id is not None:
                    span.set_attribute("mcp.request_id", request_id)
                if trace_id is not None:
                    span.set_attribute("mcp.trace_id", trace_id)
                mcp_metrics.add_counter(
                    "mcp.tool.calls_total",
                    description="Count of MCP tool invocations by tool name",
                    attributes={"tool_name": tool_name},
                )

                # Capture tenant_id if present
                tenant_id = tool_kwargs.get("tenant_id")
                if tenant_id is not None:
                    span.set_attribute("mcp.tenant_id", str(tenant_id))

                # Estimate request size (approximate)
                try:
                    req_size = len(json.dumps(tool_kwargs, default=str).encode("utf-8"))
                    span.set_attribute("mcp.tool.request.size_bytes", req_size)
                except Exception:
                    pass

                if not span.is_recording():
                    if mode in ("warn", "error"):
                        logger.warning(
                            f"Tool {tool_name} executed without active recording span. "
                            f"Mode={mode}."
                        )

                call_started_at = time.monotonic()
                try:

                    async def _invoke_with_tenant_limit() -> Any:
                        raw_tenant_id = tool_kwargs.get("tenant_id")
                        if raw_tenant_id is None:
                            return await func(*args, **tool_kwargs)

                        try:
                            tenant_id_int = int(raw_tenant_id)
                        except (TypeError, ValueError):
                            return await func(*args, **tool_kwargs)
                        if tenant_id_int <= 0:
                            return await func(*args, **tool_kwargs)

                        limiter = get_mcp_tool_tenant_limiter()
                        try:
                            async with limiter.acquire(tenant_id_int) as lease:
                                span.set_attribute("tenant.limit", int(lease.limit))
                                span.set_attribute(
                                    "tenant.active_tool_calls", int(lease.active_runs)
                                )
                                span.set_attribute("tenant.limit_exceeded", False)
                                return await func(*args, **tool_kwargs)
                        except TenantConcurrencyLimitExceeded as exc:
                            span.set_attribute("tenant.limit", int(exc.limit))
                            span.set_attribute("tenant.active_tool_calls", int(exc.active_runs))
                            span.set_attribute("tenant.limit_exceeded", True)
                            span.set_attribute("tenant.limit_kind", str(exc.limit_kind))
                            span.set_attribute(
                                "tenant.retry_after_seconds", float(exc.retry_after_seconds)
                            )
                            is_rate_limited = str(exc.limit_kind) == "rate"
                            metric_name = (
                                "mcp.tenant_rate_limited.count"
                                if is_rate_limited
                                else "mcp.tenant_limit_exceeded.count"
                            )
                            mcp_metrics.add_counter(
                                metric_name,
                                description=(
                                    "Count of MCP tenant tool rate-limit rejections"
                                    if is_rate_limited
                                    else "Count of MCP tenant tool concurrency rejections"
                                ),
                                attributes={
                                    "tool_name": tool_name,
                                    "limit_kind": str(exc.limit_kind),
                                },
                            )
                            error_message = (
                                "Tenant tool rate limit exceeded. Please retry shortly."
                                if is_rate_limited
                                else "Tenant tool concurrency limit exceeded. Please retry shortly."
                            )
                            error_code = (
                                "TENANT_TOOL_RATE_LIMIT_EXCEEDED"
                                if is_rate_limited
                                else "TENANT_TOOL_CONCURRENCY_LIMIT_EXCEEDED"
                            )
                            return tool_error_response(
                                message=error_message,
                                code=error_code,
                                category=ErrorCategory.LIMIT_EXCEEDED,
                                provider=tool_name,
                                retryable=True,
                                retry_after_seconds=float(exc.retry_after_seconds),
                            )

                    if version_parse_error:
                        span.set_attribute("mcp.tool.version_compatible", False)
                        span.set_attribute("mcp.tool.version_error", version_parse_error)
                        return tool_error_response(
                            message=(
                                "requested_tool_version must match format 'v<positive-integer>' "
                                f"(for example '{supported_tool_version}')."
                            ),
                            code=ToolVersionNegotiationErrorCode.INVALID_TOOL_VERSION_REQUEST.value,
                            category="tool_version_invalid",
                            provider=tool_name,
                            retryable=False,
                        )

                    if normalized_version is None:
                        span.set_attribute("mcp.tool.version_compatible", True)
                    else:
                        is_supported = normalized_version == supported_tool_version
                        span.set_attribute("mcp.tool.version_compatible", is_supported)
                        if not is_supported:
                            span.set_attribute(
                                "mcp.tool.version_error",
                                ToolVersionNegotiationErrorCode.UNSUPPORTED_TOOL_VERSION.value,
                            )
                            return tool_error_response(
                                message=(
                                    f"Requested tool_version '{normalized_version}' is not "
                                    f"supported for tool '{tool_name}'. "
                                    f"Supported version: '{supported_tool_version}'."
                                ),
                                code=ToolVersionNegotiationErrorCode.UNSUPPORTED_TOOL_VERSION.value,
                                category="tool_version_unsupported",
                                provider=tool_name,
                                retryable=False,
                            )

                    # Execute the tool
                    response = await _invoke_with_tenant_limit()

                    # 2. Apply Output Bounding
                    from mcp_server.utils.tool_output import bound_non_execute_tool_response

                    actual_response = response
                    is_truncated = False
                    truncation_parse_failed = False
                    resp_size = 0

                    if tool_name != "execute_sql_query":
                        bounded_response, bound_meta = bound_non_execute_tool_response(response)
                        actual_response = bounded_response
                        is_truncated = bool(bound_meta.get("truncated", False))
                        truncation_parse_failed = bool(bound_meta.get("parse_failed", False))
                    else:
                        # For execute_sql_query, we just record the size (it handles truncation)
                        is_truncated, truncation_parse_failed = _extract_truncation_signal(response)

                    actual_response = _inject_tool_version(actual_response, tool_name)
                    actual_response = _inject_request_id(actual_response, request_id)
                    actual_response = _inject_trace_id(actual_response, trace_id)
                    resp_size = len(str(actual_response).encode("utf-8"))

                    # 3. Record response attributes
                    span.set_attribute("mcp.tool.response.size_bytes", resp_size)
                    span.set_attribute("mcp.tool.response.truncated", is_truncated)
                    span.set_attribute(
                        "mcp.tool.response.truncation_parse_failed", truncation_parse_failed
                    )

                    tool_kind = "execute" if tool_name == "execute_sql_query" else "non_execute"
                    metric_attributes = {"tool_kind": tool_kind, "truncated": bool(is_truncated)}
                    mcp_metrics.record_histogram(
                        "mcp.tool.response_size_bytes",
                        float(resp_size),
                        unit="By",
                        description="Bounded MCP tool response size",
                        attributes=metric_attributes,
                    )
                    if is_truncated:
                        mcp_metrics.add_counter(
                            "mcp.tool.truncation_total",
                            description="Count of truncated MCP tool responses",
                            attributes={
                                "tool_kind": tool_kind,
                                "parse_failed": bool(truncation_parse_failed),
                            },
                        )
                    duration_ms = max(0.0, (time.monotonic() - call_started_at) * 1000.0)
                    span.set_attribute("mcp.tool.duration_ms", duration_ms)
                    mcp_metrics.record_histogram(
                        "mcp.tool.duration_ms",
                        duration_ms,
                        unit="ms",
                        description="MCP tool end-to-end duration in milliseconds",
                        attributes={"tool_name": tool_name},
                    )

                    error_category = _extract_envelope_error_category(actual_response)
                    error_code = _extract_envelope_error_code(actual_response)
                    if error_category is not None:
                        span.set_status(Status(StatusCode.ERROR))
                        span.set_attribute("mcp.tool.error.category", str(error_category))
                        if error_code is not None:
                            span.set_attribute("mcp.tool.error.code", str(error_code))
                        metric_attributes = {
                            "tool_name": tool_name,
                            "error_category": str(error_category),
                        }
                        if error_code is not None:
                            metric_attributes["error_code"] = str(error_code)
                        mcp_metrics.add_counter(
                            "mcp.tool.logical_failures_total",
                            description=(
                                "Count of MCP tool logical failures surfaced via error envelopes"
                            ),
                            attributes=metric_attributes,
                        )
                    else:
                        span.set_status(Status(StatusCode.OK))

                    return actual_response

                except Exception as e:
                    duration_ms = max(0.0, (time.monotonic() - call_started_at) * 1000.0)
                    span.set_attribute("mcp.tool.duration_ms", duration_ms)
                    mcp_metrics.record_histogram(
                        "mcp.tool.duration_ms",
                        duration_ms,
                        unit="ms",
                        description="MCP tool end-to-end duration in milliseconds",
                        attributes={"tool_name": tool_name},
                    )
                    span.record_exception(e)
                    span.set_status(Status(StatusCode.ERROR, str(e)))
                    # Try to classify error if possible
                    err_cls = getattr(e, "category", "unknown")
                    span.set_attribute("mcp.tool.error.category", err_cls)
                    raise

        return wrapper

    return decorator
