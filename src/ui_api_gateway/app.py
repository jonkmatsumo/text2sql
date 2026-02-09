import json
import logging
import re
from contextlib import asynccontextmanager
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional
from uuid import UUID, uuid4

from fastapi import BackgroundTasks, Depends, FastAPI, HTTPException, Query, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel, Field

from agent.mcp_client import MCPClient
from agent.tools import unpack_mcp_result
from common.config.env import get_env_int, get_env_list, get_env_str
from dal.control_plane import ControlPlaneDatabase
from dal.database import Database
from dal.factory import get_synth_run_store
from dal.query_target_config import QueryTargetConfigRecord
from dal.query_target_config_store import QueryTargetConfigStore
from dal.query_target_test import QueryTargetTestResult, test_query_target_connection
from dal.query_target_validation import QueryTargetValidationError, validate_query_target_payload
from ingestion.patterns.enum_detector import EnumLikeColumnDetector
from ingestion.patterns.generator import detect_candidates, generate_suggestions
from synthetic_data_gen.config import SynthConfig
from synthetic_data_gen.export import export_to_directory
from synthetic_data_gen.orchestrator import generate_tables
from ui_api_gateway.ops_jobs import OpsJobsClient, use_legacy_dal

logger = logging.getLogger(__name__)

MAX_ENRICH_BATCH_SIZE = 5

SYNTH_OUTPUT_BASE_DIR = get_env_str("SYNTH_OUTPUT_BASE_DIR", "/tmp/text2sql-synth")


# ---------------------------------------------------------------------------
# Custom Exceptions for MCP/Tool Failures
# ---------------------------------------------------------------------------


class MCPError(Exception):
    """Base exception for MCP-related failures."""

    def __init__(self, message: str, code: str = "MCP_ERROR", details: dict = None):
        """Initialize MCP error with message, code, and optional details."""
        super().__init__(message)
        self.message = message
        self.code = code
        self.details = details or {}


class MCPUpstreamError(MCPError):
    """Raised when MCP tool invocation fails (upstream/tool error)."""

    def __init__(self, message: str, details: dict = None):
        """Initialize upstream error with message and optional details."""
        super().__init__(message, code="MCP_UPSTREAM_ERROR", details=details)


class MCPTimeoutError(MCPError):
    """Raised when MCP tool invocation times out."""

    def __init__(self, message: str = "MCP tool invocation timed out", details: dict = None):
        """Initialize timeout error with message and optional details."""
        super().__init__(message, code="MCP_TIMEOUT", details=details)


class MCPConnectionError(MCPError):
    """Raised when connection to MCP server fails."""

    def __init__(self, message: str = "Failed to connect to MCP server", details: dict = None):
        """Initialize connection error with message and optional details."""
        super().__init__(message, code="MCP_CONNECTION_ERROR", details=details)


DEFAULT_MCP_URL = "http://localhost:8000/messages"
DEFAULT_MCP_TRANSPORT = "sse"
INTERNAL_AUTH_TOKEN = get_env_str("INTERNAL_AUTH_TOKEN", "")


@asynccontextmanager
async def lifespan(app):
    """Lifespan handler for gateway startup/shutdown."""
    # Startup: Initialize OpsJobsClient if not using legacy DAL
    if not use_legacy_dal():
        await OpsJobsClient.init()
        logger.info("Gateway using OpsJobsClient (new isolated DAL path)")
    else:
        logger.info("Gateway using legacy ControlPlaneDatabase path")

    await QueryTargetConfigStore.init()

    # Initialize Main Database for Ingestion Wizard (Direct Access)
    try:
        await Database.init()
        logger.info("Gateway initialized Main Database connection for Ingestion Wizard")
    except Exception as e:
        logger.warning(f"Failed to initialize Main Database: {e}")

    yield
    # Shutdown: Close OpsJobsClient connection pool
    if not use_legacy_dal():
        await OpsJobsClient.close()

    await QueryTargetConfigStore.close()

    # Close Main Database
    await Database.close()


app = FastAPI(title="Text2SQL UI API Gateway", lifespan=lifespan)


# ---------------------------------------------------------------------------
# Exception Handlers - Map MCP errors to proper HTTP status codes
# ---------------------------------------------------------------------------


ERROR_CATEGORIES = {
    "auth",
    "connectivity",
    "timeout",
    "resource_exhausted",
    "syntax",
    "unsupported",
    "transient",
    "unknown",
}

ERROR_CATEGORY_BY_CODE = {
    "missing_secret": "auth",
    "connection_error": "connectivity",
    "unsupported_provider": "unsupported",
    "athena_start_failed": "transient",
    "databricks_submit_failed": "transient",
}


def _derive_error_category(error_code: Optional[str]) -> Optional[str]:
    if not error_code:
        return None
    if error_code in ERROR_CATEGORIES:
        return error_code
    return ERROR_CATEGORY_BY_CODE.get(error_code)


def _build_error_response(exc: MCPError, request_id: str = None) -> dict:
    """Build a standardized error response payload."""
    error_category = exc.details.get("error_category") if exc.details else None
    payload = {
        "error": {
            "message": exc.message,
            "code": exc.code,
            "details": exc.details,
            "request_id": request_id,
        }
    }
    if error_category:
        payload["error"]["error_category"] = error_category
    return payload


@app.exception_handler(MCPTimeoutError)
async def mcp_timeout_handler(request: Request, exc: MCPTimeoutError) -> JSONResponse:
    """Handle MCP timeout errors with 504 Gateway Timeout."""
    request_id = request.headers.get("X-Request-ID")
    return JSONResponse(
        status_code=status.HTTP_504_GATEWAY_TIMEOUT,
        content=_build_error_response(exc, request_id),
    )


@app.exception_handler(MCPConnectionError)
async def mcp_connection_handler(request: Request, exc: MCPConnectionError) -> JSONResponse:
    """Handle MCP connection errors with 502 Bad Gateway."""
    request_id = request.headers.get("X-Request-ID")
    return JSONResponse(
        status_code=status.HTTP_502_BAD_GATEWAY,
        content=_build_error_response(exc, request_id),
    )


@app.exception_handler(MCPUpstreamError)
async def mcp_upstream_handler(request: Request, exc: MCPUpstreamError) -> JSONResponse:
    """Handle MCP upstream/tool errors with 502 Bad Gateway."""
    request_id = request.headers.get("X-Request-ID")
    return JSONResponse(
        status_code=status.HTTP_502_BAD_GATEWAY,
        content=_build_error_response(exc, request_id),
    )


@app.exception_handler(MCPError)
async def mcp_error_handler(request: Request, exc: MCPError) -> JSONResponse:
    """Handle generic MCP errors with 502 Bad Gateway."""
    request_id = request.headers.get("X-Request-ID")
    return JSONResponse(
        status_code=status.HTTP_502_BAD_GATEWAY,
        content=_build_error_response(exc, request_id),
    )


async def check_internal_auth(request: Request):
    """Verify the internal auth token if configured."""
    if not INTERNAL_AUTH_TOKEN:
        return

    token = request.headers.get("X-Internal-Token")
    if token != INTERNAL_AUTH_TOKEN:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing internal auth token",
        )


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


class QueryTargetConfigPayload(BaseModel):
    """Payload for query-target settings."""

    provider: str
    metadata: Dict[str, Any] = Field(default_factory=dict)
    auth: Dict[str, Any] = Field(default_factory=dict)
    guardrails: Dict[str, Any] = Field(default_factory=dict)
    config_id: Optional[UUID] = None


class QueryTargetActivatePayload(BaseModel):
    """Payload for activating a pending query-target config."""

    config_id: UUID


class QueryTargetConfigResponse(BaseModel):
    """Response for persisted query-target config."""

    id: UUID
    provider: str
    metadata: Dict[str, Any]
    auth: Dict[str, Any]
    guardrails: Dict[str, Any]
    status: str
    last_tested_at: Optional[str] = None
    last_test_status: Optional[str] = None
    last_error_code: Optional[str] = None
    last_error_message: Optional[str] = None
    last_error_category: Optional[str] = None


class QueryTargetSettingsResponse(BaseModel):
    """Response containing active/pending query-target configs."""

    active: Optional[QueryTargetConfigResponse] = None
    pending: Optional[QueryTargetConfigResponse] = None


class QueryTargetTestResponse(BaseModel):
    """Response for query-target connection tests."""

    ok: bool
    error_code: Optional[str] = None
    error_message: Optional[str] = None
    error_category: Optional[str] = None


class QueryTargetConfigHistoryEntry(BaseModel):
    """History entry for query-target configuration events."""

    id: UUID
    config_id: UUID
    event_type: str
    snapshot: Dict[str, Any]
    created_at: Optional[str] = None


def _to_query_target_response(record: QueryTargetConfigRecord) -> QueryTargetConfigResponse:
    return QueryTargetConfigResponse(
        id=record.id,
        provider=record.provider,
        metadata=record.metadata,
        auth=record.auth,
        guardrails=record.guardrails,
        status=record.status.value,
        last_tested_at=record.last_tested_at,
        last_test_status=record.last_test_status,
        last_error_code=record.last_error_code,
        last_error_message=record.last_error_message,
        last_error_category=_derive_error_category(record.last_error_code),
    )


app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3333",
        "http://127.0.0.1:3333",
        "http://localhost:8080",
        "http://127.0.0.1:8080",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# --- Query Target Settings ---


@app.get(
    "/settings/query-target",
    response_model=QueryTargetSettingsResponse,
    dependencies=[Depends(check_internal_auth)],
)
async def get_query_target_settings() -> QueryTargetSettingsResponse:
    """Return active/pending query-target config."""
    if not QueryTargetConfigStore.is_available():
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Query-target settings store not configured",
        )
    active = await QueryTargetConfigStore.get_active()
    pending = await QueryTargetConfigStore.get_pending()
    return QueryTargetSettingsResponse(
        active=_to_query_target_response(active) if active else None,
        pending=_to_query_target_response(pending) if pending else None,
    )


@app.get(
    "/settings/query-target/history",
    response_model=List[QueryTargetConfigHistoryEntry],
    dependencies=[Depends(check_internal_auth)],
)
async def get_query_target_history(
    limit: int = Query(50, ge=1, le=200),
) -> List[QueryTargetConfigHistoryEntry]:
    """Return recent query-target configuration history."""
    if not QueryTargetConfigStore.is_available():
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Query-target settings store not configured",
        )

    records = await QueryTargetConfigStore.list_history(limit=limit)
    entries = [
        QueryTargetConfigHistoryEntry(
            id=record.id,
            config_id=record.config_id,
            event_type=record.event_type,
            snapshot=record.snapshot,
            created_at=record.created_at,
        )
        for record in records
    ]
    entries.sort(key=lambda entry: entry.created_at or "", reverse=True)
    return entries


@app.post(
    "/settings/query-target",
    response_model=QueryTargetConfigResponse,
    dependencies=[Depends(check_internal_auth)],
)
async def upsert_query_target_settings(
    payload: QueryTargetConfigPayload,
) -> QueryTargetConfigResponse:
    """Create or update a query-target config (inactive)."""
    if not QueryTargetConfigStore.is_available():
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Query-target settings store not configured",
        )
    try:
        metadata, auth, guardrails = validate_query_target_payload(
            payload.provider, payload.metadata, payload.auth, payload.guardrails
        )
    except QueryTargetValidationError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc

    record = await QueryTargetConfigStore.upsert_config(
        provider=payload.provider.strip().lower(),
        metadata=metadata,
        auth=auth,
        guardrails=guardrails,
    )
    return _to_query_target_response(record)


@app.post(
    "/settings/query-target/activate",
    response_model=QueryTargetConfigResponse,
    dependencies=[Depends(check_internal_auth)],
)
async def activate_query_target_settings(
    payload: QueryTargetActivatePayload,
) -> QueryTargetConfigResponse:
    """Mark a query-target config as pending activation."""
    if not QueryTargetConfigStore.is_available():
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Query-target settings store not configured",
        )

    record = await QueryTargetConfigStore.get_by_id(payload.config_id)
    if not record:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Query-target config not found",
        )

    if record.last_test_status != "passed":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Query-target config must pass test-connection before activation",
        )

    await QueryTargetConfigStore.set_pending(record.id)
    pending = await QueryTargetConfigStore.get_pending()
    return _to_query_target_response(pending or record)


@app.post(
    "/settings/query-target/test-connection",
    response_model=QueryTargetTestResponse,
    dependencies=[Depends(check_internal_auth)],
)
async def test_query_target_settings(payload: QueryTargetConfigPayload) -> QueryTargetTestResponse:
    """Test query-target connection for provided settings."""
    try:
        metadata, auth, guardrails = validate_query_target_payload(
            payload.provider, payload.metadata, payload.auth, payload.guardrails
        )
    except QueryTargetValidationError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc

    result: QueryTargetTestResult = await test_query_target_connection(
        payload.provider.strip().lower(), metadata, auth, guardrails
    )

    if payload.config_id and QueryTargetConfigStore.is_available():
        await QueryTargetConfigStore.record_test_result(
            payload.config_id,
            status="passed" if result.ok else "failed",
            error_code=result.error_code,
            error_message=result.error_message,
        )

    return QueryTargetTestResponse(
        ok=result.ok,
        error_code=result.error_code,
        error_message=result.error_message,
        error_category=_derive_error_category(result.error_code),
    )


# --- Ingestion Wizard Models ---


class IngestionMetrics(BaseModel):
    """Aggregated metrics for ingestion runs."""

    total_runs: int
    total_patterns_generated: int
    total_patterns_accepted: int
    avg_acceptance_rate: float
    runs_by_day: List[Dict[str, Any]]


class IngestionRunSummary(BaseModel):
    """Summary of an ingestion run."""

    id: UUID
    started_at: datetime
    completed_at: Optional[datetime] = None
    status: str
    target_table: Optional[str] = None


class IngestionRunResponse(BaseModel):
    """Detailed response for an ingestion run."""

    id: UUID
    started_at: datetime
    completed_at: Optional[datetime] = None
    status: str
    target_table: Optional[str] = None
    config_snapshot: Dict[str, Any]
    metrics: Dict[str, Any]
    error_message: Optional[str] = None


class IngestionTemplateCreate(BaseModel):
    """Payload for creating or updating an ingestion template."""

    name: str
    description: Optional[str] = None
    config: Dict[str, Any]


class IngestionTemplate(BaseModel):
    """Detailed response for an ingestion template."""

    id: UUID
    name: str
    description: Optional[str] = None
    config: Dict[str, Any]
    created_at: datetime
    updated_at: datetime


class AnalyzeRequest(BaseModel):
    """Request payload for analyzing database for pattern candidates."""

    target_tables: Optional[List[str]] = None
    template_id: Optional[UUID] = None


class AnalyzeResponse(BaseModel):
    """Response payload for analysis result."""

    run_id: UUID
    candidates: List[Dict[str, Any]]
    warnings: List[str] = []


class EnrichRequest(BaseModel):
    """Request payload for enriching selected candidates."""

    run_id: UUID
    selected_candidates: List[Dict[str, Any]]  # subset of candidates


class EnrichResponse(BaseModel):
    """Response payload for enrichment suggestions."""

    suggestions: List[Dict[str, Any]]


class EnrichAsyncResponse(BaseModel):
    """Response payload for async enrichment trigger."""

    run_id: UUID
    job_id: UUID


# --- Synthetic Data Generation Models ---


class SynthGenerateRequest(BaseModel):
    """Request payload for starting a synthetic data generation job."""

    preset: Optional[str] = None
    config: Optional[Dict[str, Any]] = None
    output_path: Optional[str] = None
    only: Optional[List[str]] = None


class SynthGenerateResponse(BaseModel):
    """Response payload for synthetic data generation trigger."""

    run_id: UUID
    job_id: UUID


class SynthRunSummary(BaseModel):
    """Summary of a synthetic data generation run."""

    id: UUID
    started_at: datetime
    completed_at: Optional[datetime] = None
    status: str
    job_id: Optional[UUID] = None


class SynthRunResponse(BaseModel):
    """Detailed response for a synthetic data generation run."""

    id: UUID
    started_at: datetime
    completed_at: Optional[datetime] = None
    status: str
    config_snapshot: Dict[str, Any]
    output_path: Optional[str] = None
    manifest: Optional[Dict[str, Any]] = None
    metrics: Dict[str, Any]
    error_message: Optional[str] = None
    job_id: Optional[UUID] = None


class CommitRequest(BaseModel):
    """Request payload for committing patterns."""

    run_id: UUID
    approved_patterns: List[Dict[str, Any]]


class CommitResponse(BaseModel):
    """Response payload for commit operation."""

    inserted_count: int
    hydration_job_id: UUID


class RollbackRequest(BaseModel):
    """Payload for rolling back an ingestion run."""

    patterns: Optional[List[Dict[str, str]]] = None
    confirm_run_id: str


class ApproveInteractionRequest(BaseModel):
    # ... (lines below unchanged)
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


class LLMModelOption(BaseModel):
    """LLM model option payload."""

    value: str
    label: str


class LLMModelsResponse(BaseModel):
    """Response payload for available LLM models."""

    models: List[LLMModelOption]


async def _resolve_mcp_client() -> MCPClient:
    """Create an MCP client using environment configuration."""
    mcp_url = get_env_str("MCP_SERVER_URL", DEFAULT_MCP_URL)
    mcp_transport = get_env_str("MCP_TRANSPORT", DEFAULT_MCP_TRANSPORT)

    headers = {}
    if INTERNAL_AUTH_TOKEN:
        headers["X-Internal-Token"] = INTERNAL_AUTH_TOKEN

    return MCPClient(server_url=mcp_url, transport=mcp_transport, headers=headers)


def _classify_mcp_exception(exc: Exception, tool_name: str) -> MCPError:
    """Classify an exception into the appropriate MCPError subclass.

    Args:
        exc: The original exception.
        tool_name: Name of the tool that was being called.

    Returns:
        An MCPError subclass instance with appropriate classification.
    """
    exc_str = str(exc).lower()
    details = {"tool_name": tool_name, "original_error": str(exc)}

    # Timeout classification
    if "timeout" in exc_str or "timed out" in exc_str:
        return MCPTimeoutError(
            message=f"MCP tool '{tool_name}' timed out",
            details=details,
        )

    # Connection classification
    if any(
        kw in exc_str
        for kw in [
            "connection",
            "connect",
            "refused",
            "unreachable",
            "dns",
            "resolve",
            "network",
        ]
    ):
        return MCPConnectionError(
            message=f"Failed to connect to MCP server for tool '{tool_name}'",
            details=details,
        )

    # Default: upstream tool error
    return MCPUpstreamError(
        message=f"MCP tool '{tool_name}' failed: {exc}",
        details=details,
    )


async def _call_tool(tool_name: str, args: dict) -> Any:
    """Invoke an MCP tool and normalize its response.

    Raises:
        MCPTimeoutError: If the tool invocation times out.
        MCPConnectionError: If connection to MCP server fails.
        MCPUpstreamError: If the tool execution fails.
    """
    try:
        client = await _resolve_mcp_client()
        async with client.connect() as mcp:
            result = await mcp.call_tool(tool_name, arguments=args)
        return unpack_mcp_result(result)
    except (MCPError, MCPTimeoutError, MCPConnectionError, MCPUpstreamError):
        # Re-raise our own exceptions
        raise
    except Exception as exc:
        logger.warning("MCP tool '%s' failed: %s", tool_name, exc)
        raise _classify_mcp_exception(exc, tool_name) from exc


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


async def _update_job_status_running(job_id: UUID) -> None:
    """Update job status to RUNNING using configured client."""
    if use_legacy_dal():
        async with ControlPlaneDatabase.get_direct_connection() as conn:
            await conn.execute("UPDATE ops_jobs SET status = 'RUNNING' WHERE id = $1", job_id)
    else:
        await OpsJobsClient.update_status(job_id, "RUNNING")


async def _update_job_status_completed(job_id: UUID, result: Any) -> None:
    """Update job status to COMPLETED using configured client."""
    if use_legacy_dal():
        async with ControlPlaneDatabase.get_direct_connection() as conn:
            await conn.execute(
                """
                UPDATE ops_jobs
                SET status = 'COMPLETED', finished_at = NOW(), result = $2
                WHERE id = $1
                """,
                job_id,
                json.dumps(result) if not isinstance(result, str) else result,
            )
    else:
        await OpsJobsClient.update_status(job_id, "COMPLETED", result=result)


async def _update_job_status_failed(job_id: UUID, error_message: str) -> None:
    """Update job status to FAILED using configured client."""
    if use_legacy_dal():
        async with ControlPlaneDatabase.get_direct_connection() as conn:
            await conn.execute(
                """
                UPDATE ops_jobs
                SET status = 'FAILED', finished_at = NOW(), error_message = $2
                WHERE id = $1
                """,
                job_id,
                error_message,
            )
    else:
        await OpsJobsClient.update_status(job_id, "FAILED", error_message=error_message)


async def _update_job_progress(job_id: UUID, progress: Dict[str, Any]) -> None:
    """Update job progress using configured client."""
    if use_legacy_dal():
        async with ControlPlaneDatabase.get_direct_connection() as conn:
            await conn.execute(
                """
                UPDATE ops_jobs
                SET result = COALESCE(result, '{}'::jsonb) || $2
                WHERE id = $1
                """,
                job_id,
                json.dumps(progress),
            )
    else:
        await OpsJobsClient.update_progress(job_id, progress)


async def _run_enrich_job(job_id: UUID, run_id: UUID, selected_candidates: List[Dict]):
    """Background worker to execute pattern enrichment."""
    try:
        await _update_job_status_running(job_id)

        from ingestion.patterns.generator import get_openai_client

        client = await get_openai_client()
        threshold = get_env_int("ENUM_CARDINALITY_THRESHOLD", 10)
        detector = EnumLikeColumnDetector(threshold=threshold)

        all_suggestions = []
        total = len(selected_candidates)

        for i, candidate in enumerate(selected_candidates):
            # Update progress
            await _update_job_progress(job_id, {"processed": i, "total": total})

            # Enrich single candidate
            suggestions = await generate_suggestions(
                [candidate], client, detector, run_id=str(run_id)
            )
            all_suggestions.extend(suggestions)

            # Update config_snapshot with partial results
            async with Database.get_connection(tenant_id=1) as conn:
                row = await conn.fetchrow(
                    "SELECT config_snapshot FROM nlp_pattern_runs WHERE id = $1", run_id
                )
                if row:
                    snapshot = (
                        json.loads(row["config_snapshot"])
                        if isinstance(row["config_snapshot"], str)
                        else row["config_snapshot"]
                    )
                    snapshot["draft_patterns"] = all_suggestions
                    # Update ui_state
                    if "ui_state" not in snapshot:
                        snapshot["ui_state"] = {}
                    snapshot["ui_state"]["current_step"] = "review_suggestions"
                    snapshot["ui_state"]["last_updated_at"] = datetime.now().isoformat()

                    await conn.execute(
                        "UPDATE nlp_pattern_runs SET config_snapshot = $2 WHERE id = $1",
                        run_id,
                        json.dumps(snapshot),
                    )

        await _update_job_status_completed(
            job_id,
            {
                "status": "SUCCESS",
                "total_suggestions": len(all_suggestions),
                "processed": total,
                "total": total,
            },
        )
    except Exception as e:
        logger.error(f"Enrichment job {job_id} failed: {e}")
        await _update_job_status_failed(job_id, str(e))


async def _run_ops_job(job_id: UUID, job_type: str, tool_name: str, tool_args: dict):
    """Background worker to execute an ops job via MCP."""
    try:
        # 1. Update status to RUNNING
        await _update_job_status_running(job_id)

        # 2. Call MCP tool (raises MCPError on failure)
        result = await _call_tool(tool_name, tool_args)

        # 3. Finalize status as COMPLETED
        await _update_job_status_completed(job_id, result)

    except MCPError as e:
        # MCP-specific failures - log and record structured error
        logger.error("Job %s failed (MCP): %s", job_id, e.message)
        try:
            await _update_job_status_failed(job_id, e.message)
        except Exception:
            pass

    except Exception as e:
        logger.error("Job %s failed: %s", job_id, e)
        try:
            await _update_job_status_failed(job_id, str(e))
        except Exception:
            pass


async def _run_synth_job(
    job_id: UUID, run_id: UUID, config: SynthConfig, out_dir: str, only: Optional[List[str]] = None
):
    """Background worker to execute synthetic data generation."""
    logger.info(f"Starting synthetic generation job {job_id} for run {run_id}. Output: {out_dir}")
    try:
        await _update_job_status_running(job_id)

        # Progress callback to update job progress
        async def progress_callback(table_name: str, current: int, total: int):
            logger.debug(f"Job {job_id} progress: {table_name} ({current}/{total})")
            await _update_job_progress(
                job_id,
                {
                    "current_table": table_name,
                    "processed": current,
                    "total": total,
                    "percent": int((current / total) * 100) if total > 0 else 0,
                },
            )

        # 1. Run generation
        import asyncio

        loop = asyncio.get_event_loop()
        ctx, tables = await loop.run_in_executor(
            None,
            lambda: generate_tables(
                config,
                only=only,
                progress_callback=lambda n, c, t: asyncio.run_coroutine_threadsafe(
                    progress_callback(n, c, t), loop
                ),
            ),
        )

        # 2. Export results
        logger.info(f"Exporting results for job {job_id} to {out_dir}")
        manifest_path = await loop.run_in_executor(
            None, lambda: export_to_directory(ctx, config, out_dir)
        )

        # 3. Read manifest for storage
        with open(manifest_path, "r") as f:
            manifest = json.load(f)

        # 4. Update Run Record
        metrics = {
            "tables_generated": len(tables),
            "total_rows": sum(len(df) for df in tables.values()),
            "output_dir": str(out_dir),
            "version": manifest.get("version", "unknown"),
        }

        synth_run_store = get_synth_run_store()
        await synth_run_store.update_run(
            run_id,
            status="COMPLETED",
            completed_at=datetime.now(),
            manifest=manifest,
            metrics=metrics,
        )

        logger.info(f"Synthetic generation job {job_id} completed successfully. Run ID: {run_id}")
        await _update_job_status_completed(
            job_id,
            {
                "status": "SUCCESS",
                "run_id": str(run_id),
                "tables_generated": len(tables),
                "total_rows": metrics["total_rows"],
                "manifest_path": str(manifest_path),
            },
        )
    except Exception as e:
        logger.exception(f"Synthetic generation job {job_id} failed: {e}")
        await _update_job_status_failed(job_id, str(e))
        synth_run_store = get_synth_run_store()
        await synth_run_store.update_run(
            run_id, status="FAILED", completed_at=datetime.now(), error_message=str(e)
        )


async def _create_job(job_id: UUID, job_type: str) -> None:
    """Create a new ops job using configured client."""
    if use_legacy_dal():
        async with ControlPlaneDatabase.get_direct_connection() as conn:
            await conn.execute(
                "INSERT INTO ops_jobs (id, job_type, status) VALUES ($1, $2, 'PENDING')",
                job_id,
                job_type,
            )
    else:
        await OpsJobsClient.create_job(job_id, job_type)


async def _get_job(job_id: UUID) -> Optional[dict]:
    """Fetch job record using configured client."""
    if use_legacy_dal():
        async with ControlPlaneDatabase.get_direct_connection() as conn:
            row = await conn.fetchrow("SELECT * FROM ops_jobs WHERE id = $1", job_id)
            if not row:
                return None

            res = row["result"]
            if isinstance(res, str):
                try:
                    res = json.loads(res)
                except Exception:
                    res = {"raw": res}

            return {
                "id": row["id"],
                "job_type": row["job_type"],
                "status": row["status"],
                "started_at": row["started_at"],
                "finished_at": row.get("finished_at"),
                "error_message": row.get("error_message"),
                "result": res or {},
            }
    else:
        return await OpsJobsClient.get_job(job_id)


# --- Synthetic Data Generation Endpoints ---


@app.post(
    "/ops/synth/generate",
    response_model=SynthGenerateResponse,
    dependencies=[Depends(check_internal_auth)],
)
async def generate_synthetic_data(request: SynthGenerateRequest, background_tasks: BackgroundTasks):
    """Trigger synthetic data generation as a background job."""
    try:
        if request.preset:
            config = SynthConfig.preset(request.preset)
        elif request.config:
            config = SynthConfig.model_validate(request.config)
        else:
            # Default to MVP preset if nothing specified
            config = SynthConfig.preset("mvp")

        # Use provided output path or default to a timestamped dir in base dir
        if request.output_path:
            out_dir = request.output_path
        else:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            out_dir = f"{SYNTH_OUTPUT_BASE_DIR}/run_{timestamp}"

        run_id = uuid4()
        job_id = uuid4()

        # 1. Create Run Record
        synth_run_store = get_synth_run_store()
        await synth_run_store.create_run(
            config_snapshot=config.model_dump(),
            output_path=out_dir,
            status="PENDING",
            job_id=job_id,
        )

        # 2. Create Job Record
        await _create_job(job_id, "SYNTH_GENERATION")

        # 3. Start Background Task
        background_tasks.add_task(_run_synth_job, job_id, run_id, config, out_dir, request.only)

        return SynthGenerateResponse(run_id=run_id, job_id=job_id)

    except Exception as e:
        logger.error(f"Failed to trigger synthetic generation: {e}")
        raise HTTPException(status_code=400, detail=str(e))


@app.get(
    "/ops/synth/runs",
    response_model=List[SynthRunSummary],
    dependencies=[Depends(check_internal_auth)],
)
async def list_synth_runs(
    status: Optional[str] = Query(None),
    limit: int = Query(20, ge=1, le=100),
):
    """List recent synthetic generation runs."""
    synth_run_store = get_synth_run_store()
    rows = await synth_run_store.list_runs(limit=limit, status=status)
    return [
        SynthRunSummary(
            id=row["id"],
            started_at=row["started_at"],
            completed_at=row.get("completed_at"),
            status=row["status"],
            job_id=row.get("job_id"),
        )
        for row in rows
    ]


@app.get(
    "/ops/synth/runs/{run_id}",
    response_model=SynthRunResponse,
    dependencies=[Depends(check_internal_auth)],
)
async def get_synth_run(run_id: UUID):
    """Get detailed information for a specific synthetic generation run."""
    synth_run_store = get_synth_run_store()
    run = await synth_run_store.get_run(run_id)
    if not run:
        raise HTTPException(status_code=404, detail="Synthetic run not found")

    return SynthRunResponse(
        id=run["id"],
        started_at=run["started_at"],
        completed_at=run.get("completed_at"),
        status=run["status"],
        config_snapshot=run["config_snapshot"],
        output_path=run.get("output_path"),
        manifest=run.get("manifest"),
        metrics=run.get("metrics") or {},
        error_message=run.get("error_message"),
        job_id=run.get("job_id"),
    )


# --- Ingestion Wizard Endpoints ---


@app.get(
    "/ops/ingestion/templates",
    response_model=List[IngestionTemplate],
    dependencies=[Depends(check_internal_auth)],
)
async def list_ingestion_templates():
    """List all ingestion configuration templates."""
    async with ControlPlaneDatabase.get_direct_connection() as conn:
        rows = await conn.fetch("SELECT * FROM ingestion_templates ORDER BY name")
        return [
            IngestionTemplate(
                id=row["id"],
                name=row["name"],
                description=row["description"],
                config=(
                    json.loads(row["config"]) if isinstance(row["config"], str) else row["config"]
                ),
                created_at=row["created_at"],
                updated_at=row["updated_at"],
            )
            for row in rows
        ]


@app.post(
    "/ops/ingestion/templates",
    response_model=IngestionTemplate,
    dependencies=[Depends(check_internal_auth)],
)
async def create_ingestion_template(request: IngestionTemplateCreate):
    """Create a new ingestion configuration template."""
    async with ControlPlaneDatabase.get_direct_connection() as conn:
        row = await conn.fetchrow(
            """
            INSERT INTO ingestion_templates (name, description, config)
            VALUES ($1, $2, $3)
            RETURNING *
            """,
            request.name,
            request.description,
            json.dumps(request.config),
        )
        return IngestionTemplate(
            id=row["id"],
            name=row["name"],
            description=row["description"],
            config=json.loads(row["config"]) if isinstance(row["config"], str) else row["config"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )


@app.put(
    "/ops/ingestion/templates/{template_id}",
    response_model=IngestionTemplate,
    dependencies=[Depends(check_internal_auth)],
)
async def update_ingestion_template(template_id: UUID, request: IngestionTemplateCreate):
    """Update an existing ingestion configuration template."""
    async with ControlPlaneDatabase.get_direct_connection() as conn:
        row = await conn.fetchrow(
            """
            UPDATE ingestion_templates
            SET name = $1, description = $2, config = $3, updated_at = NOW()
            WHERE id = $4
            RETURNING *
            """,
            request.name,
            request.description,
            json.dumps(request.config),
            template_id,
        )
        if not row:
            raise HTTPException(status_code=404, detail="Template not found")
        return IngestionTemplate(
            id=row["id"],
            name=row["name"],
            description=row["description"],
            config=json.loads(row["config"]) if isinstance(row["config"], str) else row["config"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )


@app.delete(
    "/ops/ingestion/templates/{template_id}",
    dependencies=[Depends(check_internal_auth)],
)
async def delete_ingestion_template(template_id: UUID):
    """Delete an ingestion configuration template."""
    async with ControlPlaneDatabase.get_direct_connection() as conn:
        res = await conn.execute("DELETE FROM ingestion_templates WHERE id = $1", template_id)
        if res == "DELETE 0":
            raise HTTPException(status_code=404, detail="Template not found")
        return {"success": True}


@app.get(
    "/ops/ingestion/metrics",
    response_model=IngestionMetrics,
    dependencies=[Depends(check_internal_auth)],
)
async def get_ingestion_metrics(window: str = Query("7d")):
    """Get aggregated metrics for ingestion runs over a time window."""
    days = 7
    if window.endswith("d"):
        try:
            days = int(window[:-1])
        except ValueError:
            pass

    async with Database.get_connection(tenant_id=1) as conn:
        # Aggregates
        stats = await conn.fetchrow(
            """
            SELECT
                COUNT(*) as total_runs,
                SUM(COALESCE((metrics->>'patterns_generated')::int, 0)) as total_gen,
                SUM(COALESCE((metrics->>'patterns_accepted')::int, 0)) as total_acc
            FROM nlp_pattern_runs
            WHERE started_at > NOW() - (interval '1 day' * $1)
            """,
            days,
        )

        # Runs by day
        by_day = await conn.fetch(
            """
            SELECT
                date_trunc('day', started_at) as day,
                COUNT(*) as count,
                SUM(COALESCE((metrics->>'patterns_accepted')::int, 0)) as accepted
            FROM nlp_pattern_runs
            WHERE started_at > NOW() - (interval '1 day' * $1)
            GROUP BY 1
            ORDER BY 1
            """,
            days,
        )

        total_runs = stats["total_runs"] or 0
        total_gen = stats["total_gen"] or 0
        total_acc = stats["total_acc"] or 0
        avg_rate = (total_acc / total_gen) if total_gen > 0 else 0.0

        return IngestionMetrics(
            total_runs=total_runs,
            total_patterns_generated=total_gen,
            total_patterns_accepted=total_acc,
            avg_acceptance_rate=avg_rate,
            runs_by_day=[
                {
                    "day": r["day"].isoformat() if r["day"] else None,
                    "count": r["count"],
                    "accepted": r["accepted"] or 0,
                }
                for r in by_day
            ],
        )


@app.get(
    "/ops/ingestion/runs",
    response_model=List[IngestionRunSummary],
    dependencies=[Depends(check_internal_auth)],
)
async def list_ingestion_runs(
    status: Optional[str] = Query(None),
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
):
    """List recent ingestion runs."""
    async with Database.get_connection(tenant_id=1) as conn:
        query = "SELECT id, started_at, completed_at, status, target_table FROM nlp_pattern_runs"
        params = []
        if status:
            query += " WHERE status = $1"
            params.append(status)

        query += f" ORDER BY started_at DESC LIMIT ${len(params) + 1} OFFSET ${len(params) + 2}"
        params.extend([limit, offset])

        rows = await conn.fetch(query, *params)
        return [
            IngestionRunSummary(
                id=row["id"],
                started_at=row["started_at"],
                completed_at=row["completed_at"],
                status=row["status"],
                target_table=row["target_table"],
            )
            for row in rows
        ]


@app.get(
    "/ops/ingestion/runs/{run_id}",
    response_model=IngestionRunResponse,
    dependencies=[Depends(check_internal_auth)],
)
async def get_ingestion_run(run_id: UUID):
    """Get detailed information for a specific ingestion run."""
    async with Database.get_connection(tenant_id=1) as conn:
        row = await conn.fetchrow("SELECT * FROM nlp_pattern_runs WHERE id = $1", run_id)
        if not row:
            raise HTTPException(status_code=404, detail="Ingestion run not found")

        config_snapshot = row["config_snapshot"]
        if isinstance(config_snapshot, str):
            config_snapshot = json.loads(config_snapshot)

        metrics = row["metrics"]
        if isinstance(metrics, str):
            metrics = json.loads(metrics)

        return IngestionRunResponse(
            id=row["id"],
            started_at=row["started_at"],
            completed_at=row["completed_at"],
            status=row["status"],
            target_table=row["target_table"],
            config_snapshot=config_snapshot,
            metrics=metrics or {},
            error_message=row["error_message"],
        )


@app.get(
    "/ops/ingestion/runs/{run_id}/patterns",
    dependencies=[Depends(check_internal_auth)],
)
async def list_run_patterns(run_id: UUID):
    """List all patterns associated with an ingestion run."""
    async with Database.get_connection(tenant_id=1) as conn:
        rows = await conn.fetch(
            """
            SELECT pattern_label as label, pattern_text as pattern, action
            FROM nlp_pattern_run_items
            WHERE run_id = $1
            """,
            run_id,
        )
        return [dict(r) for r in rows]


@app.post(
    "/ops/ingestion/runs/{run_id}/rollback",
    dependencies=[Depends(check_internal_auth)],
)
async def rollback_run(run_id: UUID, request: RollbackRequest):
    """Roll back patterns created by a specific ingestion run."""
    if request.confirm_run_id != str(run_id):
        raise HTTPException(status_code=400, detail="Confirmation Run ID mismatch")

    async with Database.get_connection(tenant_id=1) as conn:
        if request.patterns:
            # Rollback selected patterns
            for p in request.patterns:
                await conn.execute(
                    "UPDATE nlp_patterns SET deleted_at = NOW() WHERE label = $1 AND pattern = $2",
                    p["label"],
                    p["pattern"],
                )
                await conn.execute(
                    """
                    UPDATE nlp_pattern_run_items
                    SET action = 'DELETED'
                    WHERE run_id = $1 AND pattern_label = $2 AND pattern_text = $3
                    """,
                    run_id,
                    p["label"],
                    p["pattern"],
                )
        else:
            # Rollback ALL created by this run
            items = await conn.fetch(
                """
                SELECT pattern_label, pattern_text
                FROM nlp_pattern_run_items
                WHERE run_id = $1 AND action = 'CREATED'
                """,
                run_id,
            )
            for item in items:
                await conn.execute(
                    "UPDATE nlp_patterns SET deleted_at = NOW() WHERE label = $1 AND pattern = $2",
                    item["pattern_label"],
                    item["pattern_text"],
                )

            await conn.execute(
                """
                UPDATE nlp_pattern_run_items
                SET action = 'DELETED'
                WHERE run_id = $1 AND action = 'CREATED'
                """,
                run_id,
            )

        await conn.execute(
            """
            UPDATE nlp_pattern_runs
            SET status = 'ROLLED_BACK', error_message = 'Rolled back by operator'
            WHERE id = $1
            """,
            run_id,
        )

        return {"success": True}


@app.post(
    "/ops/ingestion/analyze",
    response_model=AnalyzeResponse,
    dependencies=[Depends(check_internal_auth)],
)
async def analyze_source(request: AnalyzeRequest):
    """Analyze database for pattern candidates."""
    run_id = uuid4()

    target_tables = request.target_tables

    if request.template_id:
        async with ControlPlaneDatabase.get_direct_connection() as conn:
            template = await conn.fetchrow(
                "SELECT config FROM ingestion_templates WHERE id = $1", request.template_id
            )
            if template:
                config = (
                    json.loads(template["config"])
                    if isinstance(template["config"], str)
                    else template["config"]
                )
                if not target_tables:
                    target_tables = config.get("target_tables")

    # Init tools
    introspector = Database.get_schema_introspector()
    threshold = get_env_int("ENUM_CARDINALITY_THRESHOLD", 10)
    allowlist = get_env_list("ENUM_VALUE_ALLOWLIST", [])
    denylist = get_env_list("ENUM_VALUE_DENYLIST", [])

    detector = EnumLikeColumnDetector(
        threshold=threshold,
        allowlist=allowlist,
        denylist=denylist,
    )

    # 1. Detect
    try:
        # We need a connection for detection (sampling)
        # Database.get_connection() handles this
        async with Database.get_connection(tenant_id=1) as conn:
            result = await detect_candidates(
                introspector, detector, target_tables=target_tables, conn=conn
            )
    except Exception as e:
        logger.error(f"Detection failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

    candidates = result["candidates"]
    trusted_patterns = result["trusted_patterns"]

    # 2. Store Run
    ui_state = {
        "current_step": "review_candidates",
        "selected_candidates": [],
        "last_updated_at": datetime.now().isoformat(),
    }
    snapshot = {
        "candidates": candidates,
        "trusted_patterns": trusted_patterns,
        "warnings": [],
        "ui_state": ui_state,
    }

    async with Database.get_connection(tenant_id=1) as conn:
        await conn.execute(
            """
            INSERT INTO nlp_pattern_runs
            (id, status, config_snapshot, metrics)
            VALUES ($1, 'AWAITING_REVIEW', $2, '{}'::jsonb)
            """,
            run_id,
            json.dumps(snapshot),
        )

    return AnalyzeResponse(run_id=run_id, candidates=candidates, warnings=[])


@app.post(
    "/ops/ingestion/enrich",
    response_model=EnrichAsyncResponse,
    dependencies=[Depends(check_internal_auth)],
)
async def enrich_candidates(request: EnrichRequest, background_tasks: BackgroundTasks):
    """Trigger enrichment suggestions for selected candidates as a background job."""
    if len(request.selected_candidates) > MAX_ENRICH_BATCH_SIZE:
        raise HTTPException(
            status_code=400,
            detail=f"Too many candidates selected. Max batch size is {MAX_ENRICH_BATCH_SIZE}.",
        )

    job_id = uuid4()
    await _create_job(job_id, "PATTERN_ENRICHMENT")
    background_tasks.add_task(_run_enrich_job, job_id, request.run_id, request.selected_candidates)

    return EnrichAsyncResponse(run_id=request.run_id, job_id=job_id)


@app.post(
    "/ops/ingestion/commit",
    response_model=CommitResponse,
    dependencies=[Depends(check_internal_auth)],
)
async def commit_ingestion(request: CommitRequest, background_tasks: BackgroundTasks):
    """Commit patterns to DB and trigger hydration."""
    approved = request.approved_patterns
    inserted_count = 0
    patterns_generated = 0

    async with Database.get_connection(tenant_id=1) as conn:
        # Get draft count for telemetry
        row = await conn.fetchrow(
            "SELECT config_snapshot FROM nlp_pattern_runs WHERE id = $1", request.run_id
        )
        if row:
            snapshot = (
                json.loads(row["config_snapshot"])
                if isinstance(row["config_snapshot"], str)
                else row["config_snapshot"]
            )
            patterns_generated = len(snapshot.get("draft_patterns", []))

        # Insert patterns
        for p in approved:
            res = await conn.execute(
                """
                INSERT INTO nlp_patterns (id, label, pattern)
                VALUES ($1, $2, $3)
                ON CONFLICT (label, pattern) DO NOTHING
                """,
                p["id"],
                p["label"],
                p["pattern"],
            )

            # Record association
            await conn.execute(
                """
                INSERT INTO nlp_pattern_run_items
                    (run_id, pattern_id, pattern_label, pattern_text, action)
                VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT DO NOTHING
                """,
                request.run_id,
                p["id"],
                p["label"],
                p["pattern"],
                "CREATED" if res.endswith("1") else "UNCHANGED",
            )

            if res.endswith("1"):
                inserted_count += 1

        # Trigger Hydration
        job_id = uuid4()
        await _create_job(job_id, "SCHEMA_HYDRATION")
        background_tasks.add_task(_run_ops_job, job_id, "SCHEMA_HYDRATION", "hydrate_schema", {})

        # Update Run Status with Metrics & Linkage
        metrics = {
            "inserted_count": inserted_count,
            "hydration_job_id": str(job_id),
            "patterns_generated": patterns_generated,
            "patterns_accepted": len(approved),
            "patterns_rejected": max(0, patterns_generated - len(approved)),
        }

        await conn.execute(
            """
            UPDATE nlp_pattern_runs
            SET status = 'COMPLETED', completed_at = NOW(), metrics = $2
            WHERE id = $1
            """,
            request.run_id,
            json.dumps(metrics),
        )

    # Telemetry Logging
    logger.info(
        "Ingestion Commit",
        extra={
            "event": "ingestion_commit",
            "run_id": str(request.run_id),
            "patterns_generated": metrics["patterns_generated"],
            "patterns_accepted": metrics["patterns_accepted"],
            "patterns_rejected": metrics["patterns_rejected"],
            "hydration_job_id": metrics["hydration_job_id"],
        },
    )

    return CommitResponse(inserted_count=inserted_count, hydration_job_id=job_id)


@app.post(
    "/ops/schema-hydrate",
    response_model=OpsJobResponse,
    dependencies=[Depends(check_internal_auth)],
)
async def trigger_schema_hydration(background_tasks: BackgroundTasks) -> Any:
    """Trigger schema hydration job."""
    job_id = uuid4()
    await _create_job(job_id, "SCHEMA_HYDRATION")
    background_tasks.add_task(_run_ops_job, job_id, "SCHEMA_HYDRATION", "hydrate_schema", {})
    return await get_job_status(job_id)


@app.post(
    "/ops/semantic-cache/reindex",
    response_model=OpsJobResponse,
    dependencies=[Depends(check_internal_auth)],
)
async def trigger_cache_reindex(background_tasks: BackgroundTasks) -> Any:
    """Trigger semantic cache re-indexing job."""
    job_id = uuid4()
    await _create_job(job_id, "CACHE_REINDEX")
    background_tasks.add_task(_run_ops_job, job_id, "CACHE_REINDEX", "reindex_semantic_cache", {})
    return await get_job_status(job_id)


@app.get(
    "/ops/jobs/{job_id}", response_model=OpsJobResponse, dependencies=[Depends(check_internal_auth)]
)
async def get_job_status(job_id: UUID) -> Any:
    """Fetch status of a background job."""
    job = await _get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    return OpsJobResponse(
        id=job["id"],
        job_type=job["job_type"],
        status=job["status"],
        started_at=job["started_at"],
        finished_at=job.get("finished_at"),
        error_message=job.get("error_message"),
        result=job.get("result") or {},
    )


@app.get("/interactions", dependencies=[Depends(check_internal_auth)])
async def list_interactions(
    limit: int = Query(50, ge=1),
    offset: int = Query(0, ge=0),
    thumb: str = Query("All"),
    status: str = Query("All"),
) -> Any:
    """List interactions with optional UI-equivalent filters."""
    interactions = await _call_tool("list_interactions", {"limit": limit, "offset": offset})
    if not isinstance(interactions, list):
        raise MCPUpstreamError(
            message="Unexpected response format from list_interactions",
            details={"response": str(interactions)[:200]},
        )
    return _filter_interactions(interactions, thumb, status)


@app.get("/interactions/{interaction_id}")
async def get_interaction_details(interaction_id: str) -> Any:
    """Return interaction details by ID."""
    return await _call_tool("get_interaction_details", {"interaction_id": interaction_id})


@app.post("/interactions/{interaction_id}/approve", dependencies=[Depends(check_internal_auth)])
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
    if not isinstance(examples, list):
        raise MCPUpstreamError(
            message="Unexpected response format from list_approved_examples",
            details={"response": str(examples)[:200]},
        )
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


@app.get("/llm/models", response_model=LLMModelsResponse)
async def list_llm_models(provider: str = Query(...)) -> Any:
    """List available LLM models for a provider."""
    from agent.llm_client import get_available_models

    models = get_available_models(provider)
    return {"models": [{"value": model, "label": model} for model in models]}


@app.post("/ops/patterns/generate")
async def generate_patterns(request: PatternGenerateRequest) -> Any:
    """Trigger pattern generation via MCP tool."""
    return await _call_tool("generate_patterns", {"dry_run": request.dry_run})


@app.get("/ops/patterns/generate/stream")
async def stream_generate_patterns(dry_run: bool = Query(False)) -> Any:
    """Stream pattern generation logs as Server-Sent Events."""

    async def event_stream():
        run_id: Optional[str] = None
        try:
            from dal.factory import get_pattern_run_store
            from mcp_server.services.ops.maintenance import MaintenanceService

            async for log in MaintenanceService.generate_patterns(dry_run=dry_run):
                if run_id is None and "Run ID:" in log:
                    match = re.search(r"Run ID: ([a-f0-9\\-]+)", log)
                    if match:
                        run_id = match.group(1)
                payload = json.dumps({"message": log})
                yield f"data: {payload}\n\n"

            result: Dict[str, Any] = {"success": True}
            if run_id:
                run_store = get_pattern_run_store()
                run = await run_store.get_run(run_id)
                if run:
                    result = {
                        "success": run.get("status") == "COMPLETED",
                        "run_id": str(run_id),
                        "status": run.get("status"),
                        "metrics": run.get("metrics"),
                        "error": run.get("error_message"),
                    }

            yield f"event: complete\ndata: {json.dumps(result)}\n\n"
        except Exception as exc:
            payload = json.dumps({"error": str(exc)})
            yield f"event: error\ndata: {payload}\n\n"

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


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
