"""HTTP service for running the Text2SQL agent."""

import re
import uuid
from typing import Any, Optional

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from agent.graph import run_agent_with_tracing
from agent.telemetry import telemetry

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


_TRACE_ID_RE = re.compile(r"^[0-9a-f]{32}$")


@app.post("/agent/run", response_model=AgentRunResponse)
async def run_agent(request: AgentRunRequest) -> AgentRunResponse:
    """Run the agent and return UI-compatible results."""
    thread_id = request.thread_id or str(uuid.uuid4())
    try:
        state = await run_agent_with_tracing(
            question=request.question,
            tenant_id=request.tenant_id,
            thread_id=thread_id,
        )

        response_text = None
        if state.get("messages"):
            response_text = state["messages"][-1].content
        if state.get("clarification_question"):
            response_text = state["clarification_question"]

        trace_id = telemetry.get_current_trace_id()
        if trace_id and not _TRACE_ID_RE.fullmatch(trace_id):
            trace_id = None

        return AgentRunResponse(
            sql=state.get("current_sql"),
            result=state.get("query_result"),
            response=response_text,
            error=state.get("error"),
            from_cache=state.get("from_cache", False),
            cache_similarity=state.get("cache_similarity"),
            interaction_id=state.get("interaction_id"),
            trace_id=trace_id,
            viz_spec=state.get("viz_spec"),
            viz_reason=state.get("viz_reason"),
        )
    except Exception as exc:
        return AgentRunResponse(error=str(exc), trace_id=None)
