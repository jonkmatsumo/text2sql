"""HTTP service for running the Text2SQL agent."""

import asyncio
import re
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Optional

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from agent.replay_bundle import (
    ValidationError,
    build_replay_bundle,
    serialize_replay_bundle,
    validate_replay_bundle,
)
from agent.telemetry import telemetry
from common.config.env import get_env_bool, get_env_str
from common.sanitization.text import redact_sensitive_info

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
    replay_bundle: Optional[dict[str, Any]] = None
    replay_allow_external_calls: bool = False


class AgentRunResponse(BaseModel):
    """Response payload for agent execution."""

    sql: Optional[str] = None
    result: Optional[Any] = None
    response: Optional[str] = None
    error: Optional[str] = None
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
    empty_result_guidance: Optional[str] = None
    decision_summary: Optional[dict] = None
    retry_correction_summary: Optional[dict] = None
    replay_bundle: Optional[dict[str, Any]] = None
    replay_bundle_json: Optional[str] = None
    replay_metadata: Optional[dict[str, Any]] = None


_TRACE_ID_RE = re.compile(r"^[0-9a-f]{32}$")


@app.post("/agent/run", response_model=AgentRunResponse)
async def run_agent(request: AgentRunRequest) -> AgentRunResponse:
    """Run the agent and return UI-compatible results."""
    thread_id = request.thread_id or str(uuid.uuid4())
    try:
        global run_agent_with_tracing
        if run_agent_with_tracing is None:
            from agent.graph import run_agent_with_tracing as _run_agent_with_tracing

            run_agent_with_tracing = _run_agent_with_tracing

        timeout_seconds = request.timeout_seconds or 30.0
        deadline_ts = time.monotonic() + timeout_seconds
        replay_mode = (get_env_str("AGENT_REPLAY_MODE", "off") or "off").strip().lower()
        if replay_mode not in {"off", "record", "replay"}:
            replay_mode = "off"

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
                    replay_bundle=bundle,
                ),
                timeout=timeout_seconds,
            )

        replay_bundle_payload = None
        replay_bundle_json = None
        replay_metadata = None
        trace_id = None
        provenance = None

        if replay_mode == "replay":
            if not request.replay_bundle:
                return AgentRunResponse(
                    error="Replay mode requires a replay_bundle payload.",
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
            if replay_mode == "record":
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

        response_text = None
        if state.get("messages"):
            response_text = state["messages"][-1].content
        if state.get("clarification_question"):
            response_text = state["clarification_question"]

        if replay_mode != "replay" or request.replay_allow_external_calls:
            trace_id = telemetry.get_current_trace_id()
            if trace_id and not _TRACE_ID_RE.fullmatch(trace_id):
                trace_id = None
        if get_env_bool("AGENT_RESPONSE_PROVENANCE_METADATA", False) and isinstance(state, dict):
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

        return AgentRunResponse(
            sql=state.get("current_sql"),
            result=state.get("query_result"),
            response=response_text,
            error=redact_sensitive_info(state.get("error")) if state.get("error") else None,
            from_cache=state.get("from_cache", False),
            cache_similarity=state.get("cache_similarity"),
            interaction_id=state.get("interaction_id"),
            trace_id=trace_id,
            viz_spec=state.get("viz_spec"),
            viz_reason=state.get("viz_reason"),
            provenance=provenance,
            result_completeness=state.get("result_completeness"),
            error_metadata=state.get("error_metadata"),
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
            empty_result_guidance=state.get("empty_result_guidance"),
            decision_summary=state.get("decision_summary") if include_decision_debug else None,
            retry_correction_summary=(
                state.get("retry_correction_summary") if include_decision_debug else None
            ),
            replay_bundle=replay_bundle_payload,
            replay_bundle_json=replay_bundle_json,
            replay_metadata=replay_metadata,
        )
    except asyncio.TimeoutError:
        return AgentRunResponse(error="Request timed out.", trace_id=None)
    except Exception as exc:
        from common.sanitization.text import redact_sensitive_info as _redact

        return AgentRunResponse(error=_redact(str(exc)), trace_id=None)
