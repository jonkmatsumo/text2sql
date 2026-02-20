"""HTTP service for running the Text2SQL agent."""

import asyncio
import json
import re
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Optional

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from agent.audit import AuditEventSource, AuditEventType, emit_audit_event, get_audit_event_buffer
from agent.replay_bundle import (
    ValidationError,
    build_replay_bundle,
    serialize_replay_bundle,
    validate_replay_bundle,
)
from agent.telemetry import telemetry
from common.config.env import get_env_bool, get_env_str
from common.errors.error_codes import ErrorCode, canonical_error_code_for_category, parse_error_code
from common.models.error_metadata import ErrorCategory
from common.observability.metrics import agent_metrics
from common.observability.monitor import RunSummary, agent_monitor
from common.sanitization.text import redact_sensitive_info
from common.tenancy.limits import TenantConcurrencyLimitExceeded, get_agent_run_tenant_limiter

try:
    from agent.graph import run_agent_with_tracing
except Exception:
    run_agent_with_tracing = None

app = FastAPI(title="Text2SQL Agent Service")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3333",
        "http://127.0.0.1:3333",
        "http://localhost:3000",
        "http://127.0.0.1:3000",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
async def validate_startup_configuration() -> None:
    """Fail fast on invalid runtime flag combinations."""
    from common.config.sanity import validate_runtime_configuration

    validate_runtime_configuration()


class AgentRunRequest(BaseModel):
    """Request payload for agent execution."""

    question: str
    tenant_id: int = Field(..., ge=1)
    thread_id: Optional[str] = None
    timeout_seconds: Optional[float] = Field(default=None, gt=0)
    page_token: Optional[str] = None
    page_size: Optional[int] = Field(default=None, gt=0)
    replay_mode: bool = False
    replay_bundle: Optional[dict[str, Any]] = None
    replay_allow_external_calls: bool = False
    replay_integrity_check: bool = False


class AgentRunResponse(BaseModel):
    """Response payload for agent execution."""

    sql: Optional[str] = None
    result: Optional[Any] = None
    response: Optional[str] = None
    error: Optional[str] = None
    error_code: Optional[str] = None
    from_cache: bool = False
    cache_similarity: Optional[float] = None
    interaction_id: Optional[str] = None
    trace_id: Optional[str] = None
    viz_spec: Optional[dict] = None
    viz_reason: Optional[str] = None
    provenance: Optional[dict] = None
    result_completeness: Optional[dict] = None
    error_metadata: Optional[dict] = None
    retry_summary: Optional[dict] = None
    capability_summary: Optional[dict] = None
    validation_summary: Optional[dict] = None
    validation_report: Optional[dict] = None
    empty_result_guidance: Optional[str] = None
    decision_summary: Optional[dict] = None
    retry_correction_summary: Optional[dict] = None
    replay_bundle: Optional[dict[str, Any]] = None
    replay_bundle_json: Optional[str] = None
    replay_metadata: Optional[dict[str, Any]] = None


class AgentDiagnosticsResponse(BaseModel):
    """Operator-safe runtime diagnostics."""

    diagnostics_schema_version: int = 1
    active_database_provider: Optional[str] = None
    retry_policy: dict[str, Any]
    schema_cache_ttl_seconds: int
    runtime_indicators: dict[str, Any]
    enabled_flags: dict[str, Any]
    monitor_snapshot: Optional[dict[str, Any]] = None
    run_summary_store: Optional[dict[str, Any]] = None
    audit_events: Optional[list[dict[str, Any]]] = None
    debug: Optional[dict[str, Any]] = None
    self_test: Optional[dict[str, Any]] = None


class AuditDiagnosticsResponse(BaseModel):
    """Bounded operator-safe audit event diagnostics."""

    recent_events: list[dict[str, Any]]


_TRACE_ID_RE = re.compile(r"^[0-9a-f]{32}$")


def _canonical_response_error_code(
    *,
    error: Optional[str],
    error_category: Any = None,
    error_metadata: Optional[dict[str, Any]] = None,
) -> Optional[str]:
    """Resolve canonical error_code for external API responses."""
    if not error:
        return None

    if isinstance(error_metadata, dict):
        metadata_error_code = error_metadata.get("error_code")
        if metadata_error_code:
            return parse_error_code(metadata_error_code).value

    if error_category is None and isinstance(error_metadata, dict):
        error_category = error_metadata.get("category")

    if error_category is not None:
        return canonical_error_code_for_category(error_category).value

    return ErrorCode.INTERNAL_ERROR.value


def _collect_replay_integrity_mismatches(
    *,
    state: dict[str, Any],
    replay_bundle_payload: Optional[dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    """Compare replay integrity markers between captured bundle and current run."""
    if not isinstance(replay_bundle_payload, dict):
        return {"replay_bundle": {"expected": "present", "actual": "missing"}}

    mismatches: dict[str, dict[str, Any]] = {}
    integrity = replay_bundle_payload.get("integrity")
    if not isinstance(integrity, dict):
        return {"integrity": {"expected": "present", "actual": "missing"}}

    expected_schema = integrity.get("schema_snapshot_id")
    if expected_schema is None and isinstance(replay_bundle_payload.get("schema_context"), dict):
        expected_schema = replay_bundle_payload["schema_context"].get("schema_snapshot_id")
    actual_schema = state.get("schema_snapshot_id") or state.get("pinned_schema_snapshot_id")
    if expected_schema is not None and str(expected_schema) != str(actual_schema):
        mismatches["schema_snapshot_id"] = {"expected": expected_schema, "actual": actual_schema}

    expected_policy = integrity.get("policy_snapshot_id")
    policy_snapshot = state.get("policy_snapshot")
    actual_policy = (
        policy_snapshot.get("snapshot_id") if isinstance(policy_snapshot, dict) else None
    )
    if expected_policy is not None and str(expected_policy) != str(actual_policy):
        mismatches["policy_snapshot_id"] = {"expected": expected_policy, "actual": actual_policy}

    expected_hash = integrity.get("decision_summary_hash")
    run_decision_summary = state.get("run_decision_summary")
    actual_hash = (
        run_decision_summary.get("decision_summary_hash")
        if isinstance(run_decision_summary, dict)
        else None
    )
    if expected_hash is not None and str(expected_hash) != str(actual_hash):
        mismatches["decision_summary_hash"] = {"expected": expected_hash, "actual": actual_hash}

    return mismatches


class GenerateSQLRequest(BaseModel):
    """Request payload for SQL generation only."""

    question: str
    tenant_id: int = Field(..., ge=1)
    thread_id: Optional[str] = None
    timeout_seconds: Optional[float] = Field(default=None, gt=0)
    replay_mode: bool = False
    replay_bundle: Optional[dict[str, Any]] = None


class ExecuteSQLRequest(BaseModel):
    """Request payload for executing specific SQL."""

    question: str
    sql: str
    tenant_id: int = Field(..., ge=1)
    thread_id: Optional[str] = None
    timeout_seconds: Optional[float] = Field(default=None, gt=0)
    page_token: Optional[str] = None
    page_size: Optional[int] = Field(default=None, gt=0)


@app.post("/agent/generate_sql", response_model=AgentRunResponse)
async def generate_sql(request: GenerateSQLRequest) -> AgentRunResponse:
    """Generate SQL for a question without executing it."""
    # Reuse run_agent logic but with generate_only=True
    # For now, we'll wrap run_agent logic or factor it out.
    # To avoid massive refactoring, we'll just implement the core call here.
    return await _run_agent_internal(
        question=request.question,
        tenant_id=request.tenant_id,
        thread_id=request.thread_id,
        timeout_seconds=request.timeout_seconds,
        replay_mode=request.replay_mode,
        replay_bundle=request.replay_bundle,
        generate_only=True,
    )


@app.post("/agent/execute_sql", response_model=AgentRunResponse)
async def execute_sql(request: ExecuteSQLRequest) -> AgentRunResponse:
    """Execute provided SQL for a question."""
    return await _run_agent_internal(
        question=request.question,
        tenant_id=request.tenant_id,
        thread_id=request.thread_id,
        timeout_seconds=request.timeout_seconds,
        page_token=request.page_token,
        page_size=request.page_size,
        current_sql=request.sql,
        from_cache=True,  # Treat provided SQL as "cached" to bypass generation
    )


async def _run_agent_internal(
    question: str,
    tenant_id: int,
    thread_id: Optional[str] = None,
    timeout_seconds: Optional[float] = None,
    page_token: Optional[str] = None,
    page_size: Optional[int] = None,
    replay_mode: bool = False,
    replay_bundle: Optional[dict[str, Any]] = None,
    generate_only: bool = False,
    current_sql: Optional[str] = None,
    from_cache: bool = False,
) -> AgentRunResponse:
    """Run agent internal logic."""
    thread_id = thread_id or str(uuid.uuid4())
    limiter = get_agent_run_tenant_limiter()
    try:
        async with limiter.acquire(tenant_id) as lease:
            telemetry.update_current_trace(
                {
                    "tenant.active_runs": lease.active_runs,
                    "tenant.limit": lease.limit,
                    "tenant.limit_exceeded": False,
                }
            )
            global run_agent_with_tracing
            if run_agent_with_tracing is None:
                from agent.graph import run_agent_with_tracing as _run_agent_with_tracing

                run_agent_with_tracing = _run_agent_with_tracing

            timeout_seconds = timeout_seconds or 30.0
            deadline_ts = time.monotonic() + timeout_seconds

            state = await asyncio.wait_for(
                run_agent_with_tracing(
                    question=question,
                    tenant_id=tenant_id,
                    thread_id=thread_id,
                    timeout_seconds=timeout_seconds,
                    deadline_ts=deadline_ts,
                    page_token=page_token,
                    page_size=page_size,
                    interactive_session=True,
                    replay_mode=replay_mode,
                    replay_bundle=replay_bundle,
                    generate_only=generate_only,
                    current_sql=current_sql,
                    from_cache=from_cache,
                ),
                timeout=timeout_seconds,
            )

            # Construct response (simplified sharing with run_agent)
            response_text = None
            if state.get("messages"):
                response_text = state["messages"][-1].content
            if state.get("clarification_question"):
                response_text = state["clarification_question"]

            trace_id = telemetry.get_current_trace_id()
            if trace_id and not _TRACE_ID_RE.fullmatch(trace_id):
                trace_id = None

            sanitized_error = (
                redact_sensitive_info(state.get("error")) if state.get("error") else None
            )
            error_metadata = state.get("error_metadata")

            return AgentRunResponse(
                sql=state.get("current_sql"),
                result=state.get("query_result"),
                response=response_text,
                error=sanitized_error,
                error_code=_canonical_response_error_code(
                    error=sanitized_error,
                    error_category=state.get("error_category"),
                    error_metadata=error_metadata,
                ),
                from_cache=state.get("from_cache", False),
                cache_similarity=state.get("cache_similarity"),
                interaction_id=state.get("interaction_id"),
                trace_id=trace_id,
                viz_spec=state.get("viz_spec"),
                viz_reason=state.get("viz_reason"),
                provenance=None,  # Populate if needed
                result_completeness=state.get("result_completeness"),
                error_metadata=error_metadata,
                retry_summary=state.get("retry_summary"),
                decision_summary=state.get("decision_summary"),
                retry_correction_summary=state.get("retry_correction_summary"),
            )
    except TenantConcurrencyLimitExceeded:
        agent_monitor.increment("tenant_limit_exceeded")
        return AgentRunResponse(
            error="Tenant concurrency limit exceeded.",
            error_code=ErrorCode.DB_TIMEOUT.value,
            error_metadata={
                "category": ErrorCategory.LIMIT_EXCEEDED.value,
                "code": "TENANT_CONCURRENCY_LIMIT_EXCEEDED",
                "error_code": ErrorCode.DB_TIMEOUT.value,
            },
        )
    except asyncio.TimeoutError:
        return AgentRunResponse(
            error="Request timed out.",
            error_code=ErrorCode.DB_TIMEOUT.value,
            trace_id=None,
        )
    except Exception as exc:
        from common.sanitization.text import redact_sensitive_info as _redact

        return AgentRunResponse(
            error=_redact(str(exc)),
            error_code=ErrorCode.INTERNAL_ERROR.value,
            trace_id=None,
        )


@app.post("/agent/run", response_model=AgentRunResponse)
async def run_agent(request: AgentRunRequest) -> AgentRunResponse:
    """Run the agent and return UI-compatible results."""
    start_ts = time.time()
    thread_id = request.thread_id or str(uuid.uuid4())
    limiter = get_agent_run_tenant_limiter()
    try:
        async with limiter.acquire(request.tenant_id) as lease:
            telemetry.update_current_trace(
                {
                    "tenant.active_runs": lease.active_runs,
                    "tenant.limit": lease.limit,
                    "tenant.limit_exceeded": False,
                }
            )
            global run_agent_with_tracing
            if run_agent_with_tracing is None:
                from agent.graph import run_agent_with_tracing as _run_agent_with_tracing

                run_agent_with_tracing = _run_agent_with_tracing

            timeout_seconds = request.timeout_seconds or 30.0
            deadline_ts = time.monotonic() + timeout_seconds
            replay_bundle_mode = (get_env_str("AGENT_REPLAY_MODE", "off") or "off").strip().lower()
            if replay_bundle_mode not in {"off", "record", "replay"}:
                replay_bundle_mode = "off"

            async def _run_live_agent(
                question: str, bundle: Optional[dict[str, Any]] = None
            ) -> dict[str, Any]:
                return await asyncio.wait_for(
                    run_agent_with_tracing(
                        question=question,
                        tenant_id=request.tenant_id,
                        thread_id=thread_id,
                        timeout_seconds=timeout_seconds,
                        deadline_ts=deadline_ts,
                        page_token=request.page_token,
                        page_size=request.page_size,
                        interactive_session=True,
                        replay_mode=bool(request.replay_mode),
                        replay_bundle=bundle,
                    ),
                    timeout=timeout_seconds,
                )

            replay_bundle_payload = None
            replay_bundle_json = None
            replay_metadata = None
            trace_id = None
            provenance = None

            if replay_bundle_mode == "replay":
                if not request.replay_bundle:
                    return AgentRunResponse(
                        error="Replay mode requires a replay_bundle payload.",
                        error_code=ErrorCode.VALIDATION_ERROR.value,
                        trace_id=None,
                        replay_metadata={
                            "mode": "replay",
                            "execution": "captured_only",
                            "execution_mode": "replay",
                        },
                    )
                try:
                    replay_bundle = validate_replay_bundle(request.replay_bundle)
                except (ValidationError, ValueError) as exc:
                    return AgentRunResponse(
                        error=redact_sensitive_info(f"Invalid replay bundle: {exc}"),
                        error_code=ErrorCode.VALIDATION_ERROR.value,
                        trace_id=None,
                        replay_metadata={
                            "mode": "replay",
                            "execution": "captured_only",
                            "execution_mode": "replay",
                        },
                    )

                replay_bundle_payload = replay_bundle.model_dump(mode="json")
                replay_bundle_json = serialize_replay_bundle(replay_bundle)
                if request.replay_allow_external_calls:
                    replay_question = str(replay_bundle.prompts.get("user") or request.question)
                    # Hybrid replay: Live LLM, but still pass bundle for potentially offline tools
                    state = await _run_live_agent(replay_question, bundle=replay_bundle_payload)
                    replay_metadata = {
                        "mode": "replay",
                        "execution": "external_calls",
                        "execution_mode": "replay",
                        "source": "captured_prompt",
                    }
                else:
                    # Preferred offline replay: re-run the graph with the bundle
                    replay_question = str(replay_bundle.prompts.get("user") or request.question)
                    state = await _run_live_agent(replay_question, bundle=replay_bundle_payload)
                    replay_metadata = {
                        "mode": "replay",
                        "execution": "captured_only",
                        "execution_mode": "replay",
                        "external_calls": False,
                    }
            else:
                state = await _run_live_agent(request.question)
                if replay_bundle_mode == "record":
                    replay_bundle = build_replay_bundle(
                        question=request.question,
                        state=state,
                        request_payload=request.model_dump(mode="json"),
                    )
                    replay_bundle_payload = replay_bundle.model_dump(mode="json")
                    replay_bundle_json = serialize_replay_bundle(replay_bundle)
                    replay_metadata = {
                        "mode": "record",
                        "execution": "external_calls",
                        "execution_mode": "live",
                        "captured": True,
                    }

                # Optional file export for replay bundle
                export_dir = get_env_str("AGENT_REPLAY_EXPORT_DIR")
                if export_dir and replay_bundle_json:
                    import os

                    os.makedirs(export_dir, exist_ok=True)
                    export_path = os.path.join(
                        export_dir, f"replay_{thread_id}_{int(time.time())}.json"
                    )
                    with open(export_path, "w") as f:
                        f.write(replay_bundle_json)
                    if replay_metadata:
                        replay_metadata["export_path"] = export_path

            if (
                replay_bundle_mode == "replay"
                and request.replay_integrity_check
                and replay_bundle_payload is not None
            ):
                mismatches = _collect_replay_integrity_mismatches(
                    state=state,
                    replay_bundle_payload=replay_bundle_payload,
                )
                if mismatches:
                    run_id = state.get("run_id") if isinstance(state, dict) else None
                    emit_audit_event(
                        AuditEventType.REPLAY_MISMATCH,
                        source=AuditEventSource.AGENT,
                        tenant_id=request.tenant_id,
                        run_id=run_id,
                        error_category=ErrorCategory.INVALID_REQUEST,
                        metadata={
                            "reason_code": "replay_integrity_mismatch",
                            "decision": "reject",
                            "mismatch_fields": ",".join(sorted(mismatches.keys())),
                            "integrity_check": "failed",
                        },
                    )
                    return AgentRunResponse(
                        error="Replay integrity check failed.",
                        error_code=ErrorCode.VALIDATION_ERROR.value,
                        trace_id=None,
                        error_metadata={
                            "category": ErrorCategory.INVALID_REQUEST.value,
                            "code": "REPLAY_MISMATCH",
                            "error_code": ErrorCode.VALIDATION_ERROR.value,
                            "message": "Replay integrity check failed.",
                            "provider": "agent_service",
                            "retryable": False,
                            "is_retryable": False,
                            "details_safe": {
                                "mismatch_fields": sorted(mismatches.keys()),
                            },
                        },
                        replay_metadata={
                            "mode": "replay",
                            "execution_mode": "replay",
                            "integrity_check": "failed",
                        },
                    )
                if replay_metadata is None:
                    replay_metadata = {}
                replay_metadata["integrity_check"] = "passed"

            # Record metrics to monitor
            try:
                duration_ms = (time.time() - start_ts) * 1000.0
                err_cat = state.get("error_category")
                summary = RunSummary(
                    run_id=state.get("run_id") or thread_id,
                    timestamp=start_ts,
                    status="error" if state.get("error") else "success",
                    error_category=str(err_cat) if err_cat else None,
                    duration_ms=duration_ms,
                    tenant_id=request.tenant_id,
                    llm_calls=int(state.get("llm_calls", 0) or 0),
                    llm_tokens=int(state.get("llm_token_total", 0) or 0),
                )
                agent_monitor.record_run(summary)

                if err_cat == "LLM_CIRCUIT_OPEN":
                    agent_monitor.increment("circuit_breaker_open")
                    agent_metrics.add_counter("llm.circuit.open.count")
                elif err_cat == "LLM_RATE_LIMIT_EXCEEDED":
                    agent_monitor.increment("rate_limited")
                    agent_metrics.add_counter("llm.rate_limited.count")
            except Exception:
                pass  # Don't fail request on monitoring error

            response_text = None
            if state.get("messages"):
                response_text = state["messages"][-1].content
            if state.get("clarification_question"):
                response_text = state["clarification_question"]

            if replay_bundle_mode != "replay" or request.replay_allow_external_calls:
                trace_id = telemetry.get_current_trace_id()
                if trace_id and not _TRACE_ID_RE.fullmatch(trace_id):
                    trace_id = None
            if get_env_bool("AGENT_RESPONSE_PROVENANCE_METADATA", False) and isinstance(
                state, dict
            ):
                provider = get_env_str("QUERY_TARGET_BACKEND", "postgres")
                executed_at = datetime.now(timezone.utc).isoformat()
                rows_returned = state.get("result_rows_returned")
                if rows_returned is None and isinstance(state.get("query_result"), list):
                    rows_returned = len(state.get("query_result") or [])
                provenance = {
                    "executed_at": executed_at,
                    "provider": provider,
                    "schema_snapshot_id": state.get("schema_snapshot_id"),
                    "is_truncated": state.get("result_is_truncated"),
                    "is_limited": state.get("result_is_limited"),
                    "rows_returned": rows_returned,
                }

            include_decision_debug = get_env_bool("AGENT_DEBUG_DECISION_SUMMARY", False) is True

            sanitized_error = (
                redact_sensitive_info(state.get("error")) if state.get("error") else None
            )
            error_metadata = state.get("error_metadata")

            return AgentRunResponse(
                sql=state.get("current_sql"),
                result=state.get("query_result"),
                response=response_text,
                error=sanitized_error,
                error_code=_canonical_response_error_code(
                    error=sanitized_error,
                    error_category=state.get("error_category"),
                    error_metadata=error_metadata,
                ),
                from_cache=state.get("from_cache", False),
                cache_similarity=state.get("cache_similarity"),
                interaction_id=state.get("interaction_id"),
                trace_id=trace_id,
                viz_spec=state.get("viz_spec"),
                viz_reason=state.get("viz_reason"),
                provenance=provenance,
                result_completeness=state.get("result_completeness"),
                error_metadata=error_metadata,
                retry_summary=state.get("retry_summary"),
                capability_summary={
                    "required": state.get("result_capability_required"),
                    "supported": state.get("result_capability_supported"),
                    "fallback_policy": state.get("result_fallback_policy"),
                    "fallback_applied": state.get("result_fallback_applied"),
                    "fallback_mode": state.get("result_fallback_mode"),
                },
                validation_summary={
                    "ast_valid": state.get("ast_validation_result", {}).get("is_valid"),
                    "schema_drift_suspected": state.get("schema_drift_suspected"),
                    "missing_identifiers": state.get("missing_identifiers"),
                },
                validation_report=(
                    state.get("validation_report") if include_decision_debug else None
                ),
                empty_result_guidance=state.get("empty_result_guidance"),
                decision_summary=state.get("decision_summary") if include_decision_debug else None,
                retry_correction_summary=(
                    state.get("retry_correction_summary") if include_decision_debug else None
                ),
                replay_bundle=replay_bundle_payload,
                replay_bundle_json=replay_bundle_json,
                replay_metadata=replay_metadata,
            )
    except TenantConcurrencyLimitExceeded as exc:
        agent_monitor.increment("tenant_limit_exceeded")
        if exc.limit_kind == "rate":
            agent_monitor.increment("rate_limited")
        metric_name = (
            "tenant.rate_limited.count"
            if exc.limit_kind == "rate"
            else "tenant.limit_exceeded.count"
        )
        agent_metrics.add_counter(
            metric_name,
            attributes={
                "tenant_id": request.tenant_id,
                "limit_kind": str(exc.limit_kind),
            },
        )
        is_rate_limited = exc.limit_kind == "rate"
        error_code = (
            "TENANT_RATE_LIMIT_EXCEEDED" if is_rate_limited else "TENANT_CONCURRENCY_LIMIT_EXCEEDED"
        )
        error_message = (
            "Tenant rate limit exceeded. Please retry shortly."
            if is_rate_limited
            else "Tenant concurrency limit exceeded. Please retry shortly."
        )
        telemetry.update_current_trace(
            {
                "tenant.active_runs": exc.active_runs,
                "tenant.limit": exc.limit,
                "tenant.limit_exceeded": True,
                "tenant.limit_kind": str(exc.limit_kind),
                "tenant.retry_after_seconds": float(exc.retry_after_seconds),
            }
        )
        return AgentRunResponse(
            error=error_message,
            error_code=canonical_error_code_for_category(ErrorCategory.LIMIT_EXCEEDED).value,
            trace_id=None,
            error_metadata={
                "category": ErrorCategory.LIMIT_EXCEEDED.value,
                "code": error_code,
                "error_code": canonical_error_code_for_category(ErrorCategory.LIMIT_EXCEEDED).value,
                "message": error_message,
                "provider": "agent_service",
                "retryable": True,
                "is_retryable": True,
                "retry_after_seconds": exc.retry_after_seconds,
                "details_safe": {
                    "limit_kind": str(exc.limit_kind),
                    "tenant_active_runs": exc.active_runs,
                    "tenant_limit": exc.limit,
                },
            },
        )
    except asyncio.TimeoutError:
        return AgentRunResponse(
            error="Request timed out.",
            error_code=ErrorCode.DB_TIMEOUT.value,
            trace_id=None,
        )
    except Exception as exc:
        from common.sanitization.text import redact_sensitive_info as _redact

        return AgentRunResponse(
            error=_redact(str(exc)),
            error_code=ErrorCode.INTERNAL_ERROR.value,
            trace_id=None,
        )


@app.get("/agent/diagnostics", response_model=AgentDiagnosticsResponse)
def get_agent_diagnostics(
    debug: bool = False,
    self_test: bool = False,
    recent_runs_limit: int = 20,
    run_id: Optional[str] = None,
    audit_limit: int = 100,
    audit_run_id: Optional[str] = None,
) -> AgentDiagnosticsResponse:
    """Return non-sensitive runtime diagnostics for operators."""
    from agent.state.run_summary_store import get_run_summary_store
    from common.config.diagnostics import build_operator_diagnostics
    from common.config.diagnostics_self_test import run_diagnostics_self_test
    from common.observability.monitor import agent_monitor

    payload = build_operator_diagnostics(debug=debug)
    payload["monitor_snapshot"] = agent_monitor.get_snapshot()
    # Apply limit to monitor snapshot manualy since it's in-memory
    b_limit = max(0, min(200, int(recent_runs_limit)))
    if payload["monitor_snapshot"] and "recent_runs" in payload["monitor_snapshot"]:
        all_runs = payload["monitor_snapshot"]["recent_runs"]
        payload["monitor_snapshot"]["recent_runs"] = all_runs[:b_limit]
        payload["monitor_snapshot"]["truncated"] = len(all_runs) > b_limit
    summary_store = get_run_summary_store()
    bounded_limit = max(0, min(200, int(recent_runs_limit)))
    recent_runs = summary_store.list_recent(limit=bounded_limit)
    payload["run_summary_store"] = {
        "recent_runs": [
            {
                "run_id": item.get("run_id"),
                "timestamp": item.get("timestamp"),
                "terminated_reason": (item.get("summary") or {}).get("terminated_reason"),
                "tenant_id": (item.get("summary") or {}).get("tenant_id"),
                "replay_mode": bool((item.get("summary") or {}).get("replay_mode", False)),
            }
            for item in recent_runs
            if isinstance(item, dict)
        ],
        "selected_run": summary_store.get(run_id) if run_id else None,
    }
    bounded_audit_limit = max(0, min(500, int(audit_limit)))
    payload["audit_events"] = get_audit_event_buffer().list_recent(
        limit=bounded_audit_limit, run_id=audit_run_id
    )
    if self_test:
        payload["self_test"] = run_diagnostics_self_test()
    return AgentDiagnosticsResponse(**payload)


@app.get("/diagnostics/audit", response_model=AuditDiagnosticsResponse)
def get_audit_diagnostics(
    limit: int = 100, run_id: Optional[str] = None
) -> AuditDiagnosticsResponse:
    """Return recent structured audit events from bounded in-memory storage."""
    bounded_limit = max(0, min(500, int(limit)))
    return AuditDiagnosticsResponse(
        recent_events=get_audit_event_buffer().list_recent(limit=bounded_limit, run_id=run_id)
    )


@app.post("/agent/run/stream")
async def run_agent_stream(request: AgentRunRequest) -> StreamingResponse:
    """Run the agent and stream progress events via SSE."""
    thread_id = request.thread_id or str(uuid.uuid4())
    limiter = get_agent_run_tenant_limiter()

    async def event_generator():
        try:
            async with limiter.acquire(request.tenant_id) as lease:
                telemetry.update_current_trace(
                    {
                        "tenant.active_runs": lease.active_runs,
                        "tenant.limit": lease.limit,
                        "tenant.limit_exceeded": False,
                    }
                )

                # Dynamic import to avoid circular dep issues if any
                from agent.graph import run_agent_with_tracing_stream

                timeout_seconds = request.timeout_seconds or 30.0
                deadline_ts = time.monotonic() + timeout_seconds

                # Yield startup event
                startup_data = json.dumps(
                    {
                        "timestamp": time.time(),
                        "thread_id": thread_id,
                    }
                )
                yield f"event: startup\ndata: {startup_data}\n\n"

                # Replay mode not yet supported in stream path.

                try:
                    async for event in run_agent_with_tracing_stream(
                        question=request.question,
                        tenant_id=request.tenant_id,
                        thread_id=thread_id,
                        timeout_seconds=timeout_seconds,
                        deadline_ts=deadline_ts,
                        page_token=request.page_token,
                        page_size=request.page_size,
                        interactive_session=True,  # Always interactive for chat
                    ):
                        if event["event"] == "progress":
                            yield f"event: progress\ndata: {json.dumps(event['data'])}\n\n"
                        elif event["event"] == "error":
                            yield f"event: error\ndata: {json.dumps(event['data'])}\n\n"
                        elif event["event"] == "final":
                            # Yield as raw JSON (not via Pydantic).
                            payload = event["data"]

                            if payload.get("error"):
                                payload["error"] = redact_sensitive_info(payload["error"])

                            result_data = json.dumps(payload, default=str)
                            yield (f"event: result\ndata: {result_data}\n\n")

                except asyncio.TimeoutError:
                    err = json.dumps({"error": "Request timed out."})
                    yield f"event: error\ndata: {err}\n\n"
                except Exception as exc:
                    sanitized = redact_sensitive_info(str(exc))
                    err = json.dumps({"error": sanitized})
                    yield f"event: error\ndata: {err}\n\n"

        except TenantConcurrencyLimitExceeded:
            agent_monitor.increment("tenant_limit_exceeded")
            err = json.dumps(
                {
                    "error": "Tenant concurrency limit exceeded.",
                    "category": "LIMIT_EXCEEDED",
                }
            )
            yield f"event: error\ndata: {err}\n\n"
        except Exception as e:
            err = json.dumps({"error": str(e)})
            yield f"event: error\ndata: {err}\n\n"

    return StreamingResponse(event_generator(), media_type="text/event-stream")
