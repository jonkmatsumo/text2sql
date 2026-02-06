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


class AgentRunRequest(BaseModel):
    """Request payload for agent execution."""

    question: str
    tenant_id: int = Field(default=1, ge=1)
    thread_id: Optional[str] = None
    timeout_seconds: Optional[float] = Field(default=None, gt=0)
    page_token: Optional[str] = None
    page_size: Optional[int] = Field(default=None, gt=0)


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
    validation_summary: Optional[dict] = None
    empty_result_guidance: Optional[str] = None


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

        state = await asyncio.wait_for(
            run_agent_with_tracing(
                question=request.question,
                tenant_id=request.tenant_id,
                thread_id=thread_id,
                timeout_seconds=timeout_seconds,
                deadline_ts=deadline_ts,
                page_token=request.page_token,
                page_size=request.page_size,
            ),
            timeout=timeout_seconds,
        )

        response_text = None
        if state.get("messages"):
            response_text = state["messages"][-1].content
        if state.get("clarification_question"):
            response_text = state["clarification_question"]

        trace_id = telemetry.get_current_trace_id()
        if trace_id and not _TRACE_ID_RE.fullmatch(trace_id):
            trace_id = None

        provenance = None
        if get_env_bool("AGENT_RESPONSE_PROVENANCE_METADATA", False):
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
            validation_summary={
                "ast_valid": state.get("ast_validation_result", {}).get("is_valid"),
                "schema_drift_suspected": state.get("schema_drift_suspected"),
                "missing_identifiers": state.get("missing_identifiers"),
            },
            empty_result_guidance=state.get("empty_result_guidance"),
        )
    except asyncio.TimeoutError:
        return AgentRunResponse(error="Request timed out.", trace_id=None)
    except Exception as exc:
        from common.sanitization.text import redact_sensitive_info as _redact

        return AgentRunResponse(error=_redact(str(exc)), trace_id=None)
