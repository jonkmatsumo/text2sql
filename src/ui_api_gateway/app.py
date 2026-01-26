"""HTTP gateway that exposes MCP tools as JSON endpoints for UI clients."""

from typing import Any, Dict, List, Optional

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from agent.mcp_client import MCPClient
from agent.tools import unpack_mcp_result
from common.config.env import get_env_str

DEFAULT_MCP_URL = "http://localhost:8000/messages"
DEFAULT_MCP_TRANSPORT = "sse"

app = FastAPI(title="Text2SQL UI API Gateway")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://localhost:3000", "http://localhost:8501"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class ApproveInteractionRequest(BaseModel):
    """Request payload for approving an interaction."""

    corrected_sql: str
    original_sql: str
    notes: Optional[str] = None


class RejectInteractionRequest(BaseModel):
    """Request payload for rejecting an interaction."""

    reason: str = "CANNOT_FIX"
    notes: Optional[str] = None


class PublishApprovedRequest(BaseModel):
    """Request payload for publishing approved interactions."""

    limit: int = Field(default=50, ge=1)


class PinRuleUpsertRequest(BaseModel):
    """Request payload for creating or updating a pin rule."""

    tenant_id: int = Field(ge=1)
    match_type: Optional[str] = None
    match_value: Optional[str] = None
    registry_example_ids: Optional[List[str]] = None
    priority: Optional[int] = None
    enabled: Optional[bool] = None


class RecommendationRequest(BaseModel):
    """Request payload for recommendation playground."""

    query: str
    tenant_id: int = Field(default=1, ge=1)
    limit: int = Field(default=3, ge=1)
    enable_fallback: Optional[bool] = None


class PatternGenerateRequest(BaseModel):
    """Request payload for pattern generation."""

    dry_run: bool = False


class FeedbackRequest(BaseModel):
    """Request payload for feedback submission."""

    interaction_id: str
    thumb: str
    comment: Optional[str] = None


def _resolve_mcp_client() -> MCPClient:
    """Create an MCP client using environment configuration."""
    mcp_url = get_env_str("MCP_SERVER_URL", DEFAULT_MCP_URL)
    mcp_transport = get_env_str("MCP_TRANSPORT", DEFAULT_MCP_TRANSPORT)
    return MCPClient(server_url=mcp_url, transport=mcp_transport)


async def _call_tool(tool_name: str, args: dict) -> Any:
    """Invoke an MCP tool and normalize its response."""
    try:
        client = _resolve_mcp_client()
        async with client.connect() as mcp:
            result = await mcp.call_tool(tool_name, arguments=args)
        return unpack_mcp_result(result)
    except Exception as exc:
        return {"error": str(exc)}


def _filter_interactions(
    interactions: List[Dict[str, Any]], thumb_filter: str, status_filter: str
) -> List[Dict[str, Any]]:
    """Apply UI-equivalent filtering and sorting to interactions."""
    filtered: List[Dict[str, Any]] = []
    for item in interactions:
        thumb = item.get("thumb")
        if thumb_filter == "UP" and thumb != "UP":
            continue
        if thumb_filter == "DOWN" and thumb != "DOWN":
            continue
        if thumb_filter == "None" and (thumb or thumb == "UP" or thumb == "DOWN"):
            if thumb and thumb not in ["-", ""]:
                continue
        if status_filter != "All" and item.get("execution_status") != status_filter:
            continue
        filtered.append(item)

    filtered.sort(key=lambda x: x.get("created_at", ""), reverse=True)
    return filtered


def _filter_examples(
    examples: List[Dict[str, Any]], search_query: Optional[str]
) -> List[Dict[str, Any]]:
    """Apply UI-equivalent search filtering to registry examples."""
    if not search_query:
        return examples
    query = search_query.lower()
    return [
        ex
        for ex in examples
        if query in ex.get("question", "").lower() or query in ex.get("sql_query", "").lower()
    ]


@app.get("/interactions")
async def list_interactions(
    limit: int = Query(50, ge=1),
    offset: int = Query(0, ge=0),
    thumb: str = Query("All"),
    status: str = Query("All"),
) -> Any:
    """List interactions with optional UI-equivalent filters."""
    interactions = await _call_tool("list_interactions", {"limit": limit, "offset": offset})
    if isinstance(interactions, dict) and "error" in interactions:
        return interactions
    if not isinstance(interactions, list):
        return {"error": f"Unexpected response format: {interactions}"}
    return _filter_interactions(interactions, thumb, status)


@app.get("/interactions/{interaction_id}")
async def get_interaction_details(interaction_id: str) -> Any:
    """Return interaction details by ID."""
    return await _call_tool("get_interaction_details", {"interaction_id": interaction_id})


@app.post("/interactions/{interaction_id}/approve")
async def approve_interaction(interaction_id: str, request: ApproveInteractionRequest) -> Any:
    """Approve an interaction and compute resolution type."""
    resolution_type = (
        "APPROVED_AS_IS"
        if request.corrected_sql == request.original_sql
        else "APPROVED_WITH_SQL_FIX"
    )
    return await _call_tool(
        "approve_interaction",
        {
            "interaction_id": interaction_id,
            "corrected_sql": request.corrected_sql,
            "resolution_type": resolution_type,
            "reviewer_notes": request.notes,
        },
    )


@app.post("/interactions/{interaction_id}/reject")
async def reject_interaction(interaction_id: str, request: RejectInteractionRequest) -> Any:
    """Reject an interaction with a reason and optional notes."""
    return await _call_tool(
        "reject_interaction",
        {
            "interaction_id": interaction_id,
            "reason": request.reason,
            "reviewer_notes": request.notes,
        },
    )


@app.post("/registry/publish-approved")
async def publish_approved(request: PublishApprovedRequest) -> Any:
    """Publish approved interactions into the registry."""
    return await _call_tool("export_approved_to_fewshot", {"limit": request.limit})


@app.get("/registry/examples")
async def list_registry_examples(
    tenant_id: Optional[int] = Query(None, ge=1),
    limit: int = Query(50, ge=1),
    search: Optional[str] = Query(None),
) -> Any:
    """List registry examples with optional search filtering."""
    examples = await _call_tool(
        "list_approved_examples",
        {"tenant_id": tenant_id, "limit": limit},
    )
    if isinstance(examples, dict) and "error" in examples:
        return examples
    if not isinstance(examples, list):
        return {"error": f"Unexpected response format: {examples}"}
    return _filter_examples(examples, search)


@app.get("/pins")
async def list_pin_rules(tenant_id: int = Query(..., ge=1)) -> Any:
    """List pin rules for a tenant."""
    return await _call_tool("manage_pin_rules", {"operation": "list", "tenant_id": tenant_id})


@app.post("/pins")
async def create_pin_rule(request: PinRuleUpsertRequest) -> Any:
    """Create a pin rule."""
    payload = request.model_dump()
    payload["operation"] = "upsert"
    payload.pop("rule_id", None)
    return await _call_tool("manage_pin_rules", payload)


@app.patch("/pins/{rule_id}")
async def update_pin_rule(rule_id: str, request: PinRuleUpsertRequest) -> Any:
    """Update a pin rule."""
    payload = request.model_dump()
    payload["operation"] = "upsert"
    payload["rule_id"] = rule_id
    return await _call_tool("manage_pin_rules", payload)


@app.delete("/pins/{rule_id}")
async def delete_pin_rule(rule_id: str, tenant_id: int = Query(..., ge=1)) -> Any:
    """Delete a pin rule."""
    return await _call_tool(
        "manage_pin_rules",
        {"operation": "delete", "rule_id": rule_id, "tenant_id": tenant_id},
    )


@app.post("/recommendations/run")
async def run_recommendations(request: RecommendationRequest) -> Any:
    """Run recommendation tool for playground use."""
    return await _call_tool(
        "recommend_examples",
        {
            "query": request.query,
            "tenant_id": request.tenant_id,
            "limit": request.limit,
            "enable_fallback": request.enable_fallback,
        },
    )


@app.post("/ops/patterns/generate")
async def generate_patterns(request: PatternGenerateRequest) -> Any:
    """Trigger pattern generation via MCP tool."""
    return await _call_tool("generate_patterns", {"dry_run": request.dry_run})


@app.post("/ops/patterns/reload")
async def reload_patterns() -> Any:
    """Trigger pattern reload via MCP tool."""
    return await _call_tool("reload_patterns", {})


@app.post("/feedback")
async def submit_feedback(request: FeedbackRequest) -> Any:
    """Submit user feedback for an interaction."""
    return await _call_tool(
        "submit_feedback",
        {
            "interaction_id": request.interaction_id,
            "thumb": request.thumb,
            "comment": request.comment,
        },
    )
