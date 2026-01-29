import json
import logging
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional
from uuid import UUID, uuid4

from fastapi import BackgroundTasks, FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from agent.mcp_client import MCPClient
from agent.tools import unpack_mcp_result
from common.config.env import get_env_str
from dal.control_plane import ControlPlaneDatabase

logger = logging.getLogger(__name__)

DEFAULT_MCP_URL = "http://localhost:8000/messages"
DEFAULT_MCP_TRANSPORT = "sse"

app = FastAPI(title="Text2SQL UI API Gateway")


class OpsJobStatus(str, Enum):
    """Status of an operational background job."""

    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class OpsJobResponse(BaseModel):
    """Response model for job status."""

    id: UUID
    job_type: str
    status: OpsJobStatus
    started_at: datetime
    finished_at: Optional[datetime] = None
    error_message: Optional[str] = None
    result: Dict[str, Any] = {}


app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "http://127.0.0.1:5173",
        "http://localhost:3000",
        "http://127.0.0.1:3000",
        "http://localhost:8501",
        "http://127.0.0.1:8501",
        "http://localhost:8080",
        "http://127.0.0.1:8080",
    ],
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


async def _run_ops_job(job_id: UUID, job_type: str, tool_name: str, tool_args: dict):
    """Background worker to execute an ops job via MCP."""
    try:
        # 1. Update status to RUNNING
        async with ControlPlaneDatabase.get_direct_connection() as conn:
            await conn.execute(
                "UPDATE ops_jobs SET status = 'RUNNING' WHERE id = $1",
                job_id,
            )

        # 2. Call MCP tool
        result = await _call_tool(tool_name, tool_args)

        # 3. Finalize status
        status = "COMPLETED"
        error_msg = None
        if isinstance(result, dict) and "error" in result:
            status = "FAILED"
            error_msg = result["error"]

        async with ControlPlaneDatabase.get_direct_connection() as conn:
            await conn.execute(
                """
                UPDATE ops_jobs
                SET status = $2, finished_at = NOW(), error_message = $3, result = $4
                WHERE id = $1
                """,
                job_id,
                status,
                error_msg,
                json.dumps(result) if not isinstance(result, str) else result,
            )

    except Exception as e:
        logger.error(f"Job {job_id} failed: {e}")
        try:
            async with ControlPlaneDatabase.get_direct_connection() as conn:
                await conn.execute(
                    "UPDATE ops_jobs SET status = 'FAILED', error_message = $2 WHERE id = $1",
                    job_id,
                    str(e),
                )
        except Exception:
            pass


@app.post("/ops/schema-hydrate", response_model=OpsJobResponse)
async def trigger_schema_hydration(background_tasks: BackgroundTasks) -> Any:
    """Trigger schema hydration job."""
    job_id = uuid4()
    async with ControlPlaneDatabase.get_direct_connection() as conn:
        await conn.execute(
            "INSERT INTO ops_jobs (id, job_type, status) "
            "VALUES ($1, 'SCHEMA_HYDRATION', 'PENDING')",
            job_id,
        )

    background_tasks.add_task(_run_ops_job, job_id, "SCHEMA_HYDRATION", "hydrate_schema", {})
    return await get_job_status(job_id)


@app.post("/ops/semantic-cache/reindex", response_model=OpsJobResponse)
async def trigger_cache_reindex(background_tasks: BackgroundTasks) -> Any:
    """Trigger semantic cache re-indexing job."""
    job_id = uuid4()
    async with ControlPlaneDatabase.get_direct_connection() as conn:
        await conn.execute(
            "INSERT INTO ops_jobs (id, job_type, status) VALUES ($1, 'CACHE_REINDEX', 'PENDING')",
            job_id,
        )

    background_tasks.add_task(_run_ops_job, job_id, "CACHE_REINDEX", "reindex_semantic_cache", {})
    return await get_job_status(job_id)


@app.get("/ops/jobs/{job_id}", response_model=OpsJobResponse)
async def get_job_status(job_id: UUID) -> Any:
    """Fetch status of a background job."""
    async with ControlPlaneDatabase.get_direct_connection() as conn:
        row = await conn.fetchrow("SELECT * FROM ops_jobs WHERE id = $1", job_id)
        if not row:
            raise HTTPException(status_code=404, detail="Job not found")

        # Handle result parsing
        res = row["result"]
        if isinstance(res, str):
            try:
                res = json.loads(res)
            except Exception:
                res = {"raw": res}

        return OpsJobResponse(
            id=row["id"],
            job_type=row["job_type"],
            status=row["status"],
            started_at=row["started_at"],
            finished_at=row.get("finished_at"),
            error_message=row.get("error_message"),
            result=res or {},
        )


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
