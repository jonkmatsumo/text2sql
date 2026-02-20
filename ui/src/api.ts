import {
  AgentRunRequest,
  AgentRunResponse,
  FeedbackRequest,
  SpanDetail,
  SpanSummary,
  TraceDetail,
  ListTracesParams,
  PaginatedTracesResponse,
  MetricsPreviewResponse,
  TraceAggregationsResponse,
  GenerateSQLRequest,
  ExecuteSQLRequest
} from "./types";

export type {
  GenerateSQLRequest,
  ExecuteSQLRequest
};
import {
  Interaction,
  ApprovedExample,
  PinRule,
  RecommendationResult,
  PatternGenerationResult,
  PatternReloadResult,
  SynthGenerateRequest,
  SynthGenerateResponse,
  SynthRun,
  SynthRunSummary,
  OpsJobResponse,
  JobStatusResponse,
  ListRunsResponse,
  CancelJobResponse,
} from "./types/admin";
import type { DiagnosticsResponse, RunDiagnosticsResponse } from "./types/diagnostics";
import {
  agentServiceBaseUrl,
  uiApiBaseUrl,
  otelWorkerBaseUrl,
  internalAuthToken
} from "./config";
import { RUN_HISTORY_PAGE_SIZE } from "./constants/pagination";
import {
  buildContractMismatchReport,
  isInteractionArray,
  isJobStatusResponse,
  isOpsJobResponseArray,
  isDiagnosticsResponse,
  isRunDiagnosticsResponse,
  isCancelJobResponse,
} from "./utils/runtimeGuards";

const agentBase = agentServiceBaseUrl;
const uiApiBase = uiApiBaseUrl;
const otelBase = otelWorkerBaseUrl;

// ---------------------------------------------------------------------------
// API Error Handling
// ---------------------------------------------------------------------------

/**
 * Structured error response from the gateway.
 * Matches the error format: { error: { message, code, details, request_id } }
 */
export interface ApiErrorDetails {
  message: string;
  code: string;
  details?: Record<string, unknown>;
  request_id?: string;
  error_category?: string;
}

/**
 * Custom error class for API failures with structured details.
 */
export class ApiError extends Error {
  public readonly status: number;
  public readonly code: string;
  public readonly details: Record<string, unknown>;
  public readonly requestId?: string;

  constructor(
    message: string,
    status: number,
    code: string = "UNKNOWN_ERROR",
    details: Record<string, unknown> = {},
    requestId?: string
  ) {
    super(message);
    this.name = "ApiError";
    this.status = status;
    this.code = code;
    this.details = details;
    this.requestId = requestId;
  }

  /**
   * Human-readable error message for display in UI.
   */
  get displayMessage(): string {
    if (this.code === "MCP_CONNECTION_ERROR") {
      return "Backend service is unavailable. Please try again later.";
    }
    if (this.code === "MCP_TIMEOUT") {
      return "Request timed out. Please try again.";
    }
    if (this.code === "MCP_UPSTREAM_ERROR") {
      return this.message || "An error occurred while processing your request.";
    }
    return this.message;
  }

  /**
   * Check if this is a gateway/upstream error (5xx).
   */
  get isUpstreamError(): boolean {
    return this.status >= 500 && this.status < 600;
  }
}

/**
 * Extract user-friendly error message from an error object.
 * Uses ApiError.displayMessage when available for better UX.
 */
export function getErrorMessage(err: unknown): string {
  if (err instanceof ApiError) {
    return err.displayMessage;
  }
  if (err instanceof Error) {
    return err.message;
  }
  return "An unexpected error occurred";
}

/**
 * Parse error response body and throw ApiError.
 * Falls back to generic error if response cannot be parsed.
 */
async function throwApiError(response: Response, fallbackMessage: string): Promise<never> {
  let errorDetails: ApiErrorDetails | undefined;

  try {
    const body = await response.json();
    if (body?.error && typeof body.error === "object") {
      errorDetails = body.error;
    }
  } catch {
    // Response is not JSON, use fallback
  }

  if (errorDetails) {
    throw new ApiError(
      errorDetails.message || fallbackMessage,
      response.status,
      errorDetails.code,
      errorDetails.details || {},
      errorDetails.request_id
    );
  }

  throw new ApiError(fallbackMessage, response.status);
}

function getAuthHeaders(): Record<string, string> {
  const headers: Record<string, string> = {
    "Content-Type": "application/json"
  };
  if (internalAuthToken) {
    headers["X-Internal-Token"] = internalAuthToken;
  }
  return headers;
}

export async function fetchMetricsPreview(
  window: string = "1h",
  service?: string
): Promise<MetricsPreviewResponse> {
  const searchParams = new URLSearchParams({ window });
  if (service) searchParams.append("service", service);

  const url = `${otelBase}/api/v1/metrics/preview?${searchParams}`;
  const response = await fetch(url, {
    headers: getAuthHeaders()
  });
  if (!response.ok) {
    await throwApiError(response, "Failed to fetch metrics");
  }
  return response.json();
}

export async function runAgent(request: AgentRunRequest): Promise<AgentRunResponse> {
  const response = await fetch(`${agentBase}/agent/run`, {
    method: "POST",
    headers: getAuthHeaders(),
    body: JSON.stringify(request)
  });

  if (!response.ok) {
    await throwApiError(response, "Agent service error");
  }

  return response.json();
}

export async function generateSQL(request: GenerateSQLRequest): Promise<AgentRunResponse> {
  const response = await fetch(`${agentBase}/agent/generate_sql`, {
    method: "POST",
    headers: getAuthHeaders(),
    body: JSON.stringify(request)
  });

  if (!response.ok) {
    await throwApiError(response, "Failed to generate SQL");
  }

  return response.json();
}

export async function executeSQL(request: ExecuteSQLRequest): Promise<AgentRunResponse> {
  const response = await fetch(`${agentBase}/agent/execute_sql`, {
    method: "POST",
    headers: getAuthHeaders(),
    body: JSON.stringify(request)
  });

  if (!response.ok) {
    await throwApiError(response, "Failed to execute SQL");
  }

  return response.json();
}

export interface LlmModelOption {
  value: string;
  label: string;
}

export async function fetchAvailableModels(provider: string): Promise<LlmModelOption[]> {
  const params = new URLSearchParams({ provider });
  const response = await fetch(`${uiApiBase}/llm/models?${params}`, {
    headers: getAuthHeaders()
  });
  if (!response.ok) {
    await throwApiError(response, "Failed to fetch available models");
  }
  const data = await response.json();
  if (data && Array.isArray(data.models)) {
    return data.models;
  }
  return [];
}

export interface FeedbackResponse {
  status: string;
  feedback_id?: string;
}

export async function submitFeedback(request: FeedbackRequest): Promise<FeedbackResponse> {
  const response = await fetch(`${uiApiBase}/feedback`, {
    method: "POST",
    headers: getAuthHeaders(),
    body: JSON.stringify(request)
  });

  if (!response.ok) {
    await throwApiError(response, "Failed to submit feedback");
  }

  return response.json();
}

export async function fetchTraceDetail(traceId: string): Promise<TraceDetail> {
  const response = await fetch(`${otelBase}/api/v1/traces/${traceId}?include=attributes`);
  if (!response.ok) {
    await throwApiError(response, "Failed to fetch trace");
  }
  return response.json();
}

export async function fetchTraceSpans(
  traceId: string,
  limit: number = 500,
  offset: number = 0
): Promise<SpanSummary[]> {
  const response = await fetch(
    `${otelBase}/api/v1/traces/${traceId}/spans?include=attributes&limit=${limit}&offset=${offset}`
  );
  if (!response.ok) {
    await throwApiError(response, "Failed to fetch spans");
  }
  const data = await response.json();
  return data.items || [];
}

export async function fetchSpanDetail(
  traceId: string,
  spanId: string
): Promise<SpanDetail> {
  const response = await fetch(`${otelBase}/api/v1/traces/${traceId}/spans/${spanId}`);
  if (!response.ok) {
    await throwApiError(response, "Failed to fetch span details");
  }
  return response.json();
}

export async function resolveTraceByInteraction(interactionId: string): Promise<string> {
  const response = await fetch(`${otelBase}/api/v1/traces/by-interaction/${interactionId}`);
  if (!response.ok) {
    await throwApiError(response, "Failed to resolve trace");
  }
  const data = await response.json();
  return data.trace_id;
}


export async function fetchBlobContent(blobUrl: string): Promise<unknown> {
  const response = await fetch(blobUrl);
  if (!response.ok) {
    throw new Error(`Failed to fetch blob content: ${response.statusText}`);
  }
  const contentType = response.headers.get("Content-Type");
  if (contentType?.includes("application/json")) {
    return response.json();
  }
  return response.text();
}

export async function listTraces(
  params: ListTracesParams = {}
): Promise<PaginatedTracesResponse> {
  const searchParams = new URLSearchParams();
  if (params.service) searchParams.append("service", params.service);
  if (params.trace_id) searchParams.append("trace_id", params.trace_id);
  if (params.start_time_gte) searchParams.append("start_time_gte", params.start_time_gte);
  if (params.start_time_lte) searchParams.append("start_time_lte", params.start_time_lte);
  if (params.duration_min_ms != null) searchParams.append("duration_min_ms", params.duration_min_ms.toString());
  if (params.duration_max_ms != null) searchParams.append("duration_max_ms", params.duration_max_ms.toString());
  if (params.limit !== undefined) searchParams.append("limit", params.limit.toString());
  if (params.offset !== undefined) searchParams.append("offset", params.offset.toString());
  if (params.order) searchParams.append("order", params.order);

  const url = `${otelBase}/api/v1/traces${searchParams.toString() ? `?${searchParams}` : ""}`;
  const response = await fetch(url);
  if (!response.ok) {
    await throwApiError(response, "Failed to list traces");
  }
  return response.json();
}

export async function fetchTraceAggregations(params: {
  service?: string;
  trace_id?: string;
  status?: string;
  has_errors?: "yes" | "no";
  start_time_gte?: string;
  start_time_lte?: string;
  duration_min_ms?: number | null;
  duration_max_ms?: number | null;
} = {}): Promise<TraceAggregationsResponse> {
  const searchParams = new URLSearchParams();
  if (params.service) searchParams.append("service", params.service);
  if (params.trace_id) searchParams.append("trace_id", params.trace_id);
  if (params.status) searchParams.append("status", params.status);
  if (params.has_errors) searchParams.append("has_errors", params.has_errors);
  if (params.start_time_gte) searchParams.append("start_time_gte", params.start_time_gte);
  if (params.start_time_lte) searchParams.append("start_time_lte", params.start_time_lte);
  if (params.duration_min_ms != null) {
    searchParams.append("duration_min_ms", String(params.duration_min_ms));
  }
  if (params.duration_max_ms != null) {
    searchParams.append("duration_max_ms", String(params.duration_max_ms));
  }

  const url = `${otelBase}/api/v1/traces/aggregations${searchParams.toString() ? `?${searchParams}` : ""}`;
  const response = await fetch(url);
  if (!response.ok) {
    await throwApiError(response, "Failed to fetch trace aggregations");
  }
  return response.json();
}

export const AdminService = {
  async listInteractions(
    limit: number = 50,
    thumb: string = "All",
    status: string = "All"
  ): Promise<Interaction[]> {
    const params = new URLSearchParams({
      limit: limit.toString(),
      thumb,
      status
    });
    const response = await fetch(`${uiApiBase}/interactions?${params}`, {
      headers: getAuthHeaders()
    });
    if (!response.ok) await throwApiError(response, "Failed to load interactions");
    return response.json();
  },

  async getInteractionDetails(id: string): Promise<Interaction> {
    const response = await fetch(`${uiApiBase}/interactions/${id}`, {
      headers: getAuthHeaders()
    });
    if (!response.ok) await throwApiError(response, "Failed to load interaction details");
    return response.json();
  },

  async approveInteraction(
    id: string,
    correctedSql: string,
    originalSql: string,
    notes: string = ""
  ): Promise<string> {
    const response = await fetch(`${uiApiBase}/interactions/${id}/approve`, {
      method: "POST",
      headers: getAuthHeaders(),
      body: JSON.stringify({
        corrected_sql: correctedSql,
        original_sql: originalSql,
        notes
      })
    });
    if (!response.ok) await throwApiError(response, "Failed to approve interaction");
    return response.json();
  },

  async rejectInteraction(
    id: string,
    reason: string = "CANNOT_FIX",
    notes: string = ""
  ): Promise<string> {
    const response = await fetch(`${uiApiBase}/interactions/${id}/reject`, {
      method: "POST",
      headers: getAuthHeaders(),
      body: JSON.stringify({ reason, notes })
    });
    if (!response.ok) await throwApiError(response, "Failed to reject interaction");
    return response.json();
  },

  async publishApproved(limit: number = 50): Promise<{ published_count: number }> {
    const response = await fetch(`${uiApiBase}/registry/publish-approved`, {
      method: "POST",
      headers: getAuthHeaders(),
      body: JSON.stringify({ limit })
    });
    if (!response.ok) await throwApiError(response, "Failed to publish approved interactions");
    return response.json();
  },

  async listExamples(limit: number = 100, search?: string): Promise<ApprovedExample[]> {
    const params = new URLSearchParams({ limit: limit.toString() });
    if (search) params.append("search", search);
    const response = await fetch(`${uiApiBase}/registry/examples?${params}`, {
      headers: getAuthHeaders()
    });
    if (!response.ok) await throwApiError(response, "Failed to load examples");
    return response.json();
  },

  async listPins(tenantId: number): Promise<PinRule[]> {
    const params = new URLSearchParams({ tenant_id: tenantId.toString() });
    const response = await fetch(`${uiApiBase}/pins?${params}`, {
      headers: getAuthHeaders()
    });
    if (!response.ok) await throwApiError(response, "Failed to list pins");
    return response.json();
  },

  async upsertPin(data: Partial<PinRule> & { tenant_id: number }): Promise<PinRule> {
    const isUpdate = !!data.id;
    const url = isUpdate ? `${uiApiBase}/pins/${data.id}` : `${uiApiBase}/pins`;
    const response = await fetch(url, {
      method: isUpdate ? "PATCH" : "POST",
      headers: getAuthHeaders(),
      body: JSON.stringify(data)
    });
    if (!response.ok) await throwApiError(response, "Failed to upsert pin");
    return response.json();
  },

  async deletePin(id: string, tenantId: number): Promise<{ success: boolean }> {
    const params = new URLSearchParams({ tenant_id: tenantId.toString() });
    const response = await fetch(`${uiApiBase}/pins/${id}?${params}`, {
      method: "DELETE",
      headers: getAuthHeaders()
    });
    if (!response.ok) await throwApiError(response, "Failed to delete pin");
    return response.json();
  }
};

interface RunsPage {
  items: Interaction[];
  has_more?: boolean;
  total_count?: number;
}

function normalizeRunsPage(data: unknown): RunsPage | null {
  if (isInteractionArray(data)) {
    return { items: data };
  }

  if (!data || typeof data !== "object" || Array.isArray(data)) {
    return null;
  }

  const payload = data as Record<string, unknown>;
  const itemsCandidate = Array.isArray(payload.items)
    ? payload.items
    : Array.isArray(payload.runs)
      ? payload.runs
      : Array.isArray(payload.data)
        ? payload.data
        : undefined;

  if (itemsCandidate === undefined || !isInteractionArray(itemsCandidate)) {
    return null;
  }

  return {
    items: itemsCandidate,
    has_more: typeof payload.has_more === "boolean" ? payload.has_more : undefined,
    total_count: typeof payload.total_count === "number" ? payload.total_count : undefined,
  };
}

export const OpsService = {
  async runRecommendations(
    query: string,
    tenantId: number,
    limit: number,
    enableFallback: boolean
  ): Promise<RecommendationResult> {
    const response = await fetch(`${uiApiBase}/recommendations/run`, {
      method: "POST",
      headers: getAuthHeaders(),
      body: JSON.stringify({
        query,
        tenant_id: tenantId,
        limit,
        enable_fallback: enableFallback
      })
    });
    if (!response.ok) await throwApiError(response, "Recommendation run failed");
    return response.json();
  },

  async generatePatterns(dryRun: boolean = false): Promise<PatternGenerationResult> {
    const response = await fetch(`${uiApiBase}/ops/patterns/generate`, {
      method: "POST",
      headers: getAuthHeaders(),
      body: JSON.stringify({ dry_run: dryRun })
    });
    if (!response.ok) await throwApiError(response, "Pattern generation failed");
    return response.json();
  },

  async reloadPatterns(): Promise<PatternReloadResult> {
    const response = await fetch(`${uiApiBase}/ops/patterns/reload`, {
      method: "POST",
      headers: getAuthHeaders()
    });
    if (!response.ok) await throwApiError(response, "Pattern reload failed");
    return response.json();
  },

  async hydrateSchema(): Promise<{ success: boolean; job_id?: string }> {
    const response = await fetch(`${uiApiBase}/ops/schema-hydrate`, {
      method: "POST",
      headers: getAuthHeaders()
    });
    if (!response.ok) await throwApiError(response, "Schema hydration failed");
    return response.json();
  },

  async reindexCache(): Promise<{ success: boolean; job_id?: string }> {
    const response = await fetch(`${uiApiBase}/ops/semantic-cache/reindex`, {
      method: "POST",
      headers: getAuthHeaders()
    });
    if (!response.ok) await throwApiError(response, "Cache re-indexing failed");
    return response.json();
  },

  async getJobStatus(jobId: string): Promise<JobStatusResponse> {
    const response = await fetch(`${uiApiBase}/ops/jobs/${jobId}`, {
      headers: getAuthHeaders()
    });
    if (!response.ok) await throwApiError(response, "Failed to fetch job status");
    const data: unknown = await response.json();
    if (!isJobStatusResponse(data)) {
      const report = buildContractMismatchReport("OpsService.getJobStatus", data, { jobId });
      console.error("Operator API contract mismatch (getJobStatus)", report);
      throw new ApiError(
        `Received unexpected response from getJobStatus${report.ids.trace_id ? ` (trace_id=${report.ids.trace_id})` : ""}`,
        200,
        "MALFORMED_RESPONSE",
        { endpoint: "getJobStatus", surface: report.surface, ...report.ids, request_context: report.request_context }
      );
    }
    return data;
  },

  async cancelJob(jobId: string): Promise<CancelJobResponse> {
    const response = await fetch(`${uiApiBase}/ops/jobs/${jobId}/cancel`, {
      method: "POST",
      headers: getAuthHeaders()
    });
    if (!response.ok) await throwApiError(response, "Failed to cancel job");
    const data: unknown = await response.json();
    if (!isCancelJobResponse(data)) {
      const report = buildContractMismatchReport("OpsService.cancelJob", data, { jobId });
      console.error("Operator API contract mismatch (cancelJob)", report);
      throw new ApiError(
        `Received unexpected response from cancelJob${report.ids.trace_id ? ` (trace_id=${report.ids.trace_id})` : ""}`,
        200,
        "MALFORMED_RESPONSE",
        { endpoint: "cancelJob", surface: report.surface, ...report.ids, request_context: report.request_context }
      );
    }
    return data;
  },

  async listJobs(
    limit: number = 50,
    jobType?: string,
    status?: string
  ): Promise<OpsJobResponse[]> {
    const params = new URLSearchParams({ limit: limit.toString() });
    if (jobType) params.append("job_type", jobType);
    if (status) params.append("status", status);

    const response = await fetch(`${uiApiBase}/ops/jobs?${params}`, {
      headers: getAuthHeaders()
    });
    if (!response.ok) await throwApiError(response, "Failed to list jobs");
    const data: unknown = await response.json();
    if (!isOpsJobResponseArray(data)) {
      const report = buildContractMismatchReport("OpsService.listJobs", data, { limit, jobType, status });
      console.error("Operator API contract mismatch (listJobs)", report);
      throw new ApiError(
        `Received unexpected response from listJobs${report.ids.trace_id ? ` (trace_id=${report.ids.trace_id})` : ""}`,
        200,
        "MALFORMED_RESPONSE",
        { endpoint: "listJobs", surface: report.surface, ...report.ids, request_context: report.request_context }
      );
    }
    return data;
  },

  // NOTE: This assumes default backend pagination is consistent with frontend RUN_HISTORY_PAGE_SIZE.
  async listRuns(
    limit: number = RUN_HISTORY_PAGE_SIZE,
    offset: number = 0,
    status: string = "All",
    thumb: string = "All"
  ): Promise<ListRunsResponse> {
    const params = new URLSearchParams({
      limit: limit.toString(),
      offset: offset.toString(),
      status,
      thumb,
    });
    const response = await fetch(`${uiApiBase}/interactions?${params}`, {
      headers: getAuthHeaders(),
    });
    if (!response.ok) await throwApiError(response, "Failed to load runs");
    const data: unknown = await response.json();
    const page = normalizeRunsPage(data);
    if (page) {
      return {
        runs: page.items,
        has_more: page.has_more,
        total_count: page.total_count,
      };
    }

    const report = buildContractMismatchReport("OpsService.listRuns", data, { limit, offset, status, thumb });
    console.error("Operator API contract mismatch (listRuns)", report);
    throw new ApiError(
      `Received unexpected response from listRuns${report.ids.trace_id ? ` (trace_id=${report.ids.trace_id})` : ""}`,
      200,
      "MALFORMED_RESPONSE",
      { endpoint: "listRuns", surface: report.surface, ...report.ids, request_context: report.request_context }
    );
  },
};

// --- Ingestion Wizard Types & Service ---

export interface IngestionCandidate {
  table: string;
  column: string;
  values: string[];
  label: string;
  // UI state
  selected?: boolean;
}

export interface AnalyzeResponse {
  run_id: string;
  candidates: IngestionCandidate[];
  warnings: string[];
}

export interface Suggestion {
  id: string; // Canonical value
  label: string;
  pattern: string; // Synonym
  accepted?: boolean; // UI state
  is_new?: boolean; // UI state (manual add)
}

export interface EnrichAsyncResponse {
  run_id: string;
  job_id: string;
}

export interface CommitResponse {
  inserted_count: number;
  hydration_job_id: string;
}

export interface IngestionRun {
  id: string;
  started_at: string;
  completed_at?: string;
  status: string;
  target_table?: string;
  config_snapshot?: any;
  metrics?: any;
  error_message?: string;
}

export interface IngestionTemplate {
  id: string;
  name: string;
  description?: string;
  config: any;
  created_at: string;
  updated_at: string;
}

export const IngestionService = {
  async listTemplates(): Promise<IngestionTemplate[]> {
    const response = await fetch(`${uiApiBase}/ops/ingestion/templates`, {
      headers: getAuthHeaders()
    });
    if (!response.ok) await throwApiError(response, "Failed to list templates");
    return response.json();
  },

  async createTemplate(data: { name: string; description?: string; config: any }): Promise<IngestionTemplate> {
    const response = await fetch(`${uiApiBase}/ops/ingestion/templates`, {
      method: "POST",
      headers: getAuthHeaders(),
      body: JSON.stringify(data)
    });
    if (!response.ok) await throwApiError(response, "Failed to create template");
    return response.json();
  },

  async deleteTemplate(id: string): Promise<{ success: boolean }> {
    const response = await fetch(`${uiApiBase}/ops/ingestion/templates/${id}`, {
      method: "DELETE",
      headers: getAuthHeaders()
    });
    if (!response.ok) await throwApiError(response, "Failed to delete template");
    return response.json();
  },

  async getMetrics(window: string = "7d"): Promise<Record<string, unknown>> {
    const response = await fetch(`${uiApiBase}/ops/ingestion/metrics?window=${window}`, {
      headers: getAuthHeaders()
    });
    if (!response.ok) await throwApiError(response, "Failed to get ingestion metrics");
    return response.json();
  },

  async rollbackRun(runId: string, patterns?: any[]): Promise<{ success: boolean; job_id?: string }> {
    const response = await fetch(`${uiApiBase}/ops/ingestion/runs/${runId}/rollback`, {
      method: "POST",
      headers: getAuthHeaders(),
      body: JSON.stringify({ confirm_run_id: runId, patterns })
    });
    if (!response.ok) await throwApiError(response, "Rollback failed");
    return response.json();
  },

  async getRunPatterns(runId: string): Promise<Suggestion[]> {
    const response = await fetch(`${uiApiBase}/ops/ingestion/runs/${runId}/patterns`, {
      headers: getAuthHeaders()
    });
    if (!response.ok) await throwApiError(response, "Failed to list run patterns");
    return response.json();
  },

  async listRuns(status?: string): Promise<IngestionRun[]> {
    const params = new URLSearchParams();
    if (status) params.append("status", status);
    const response = await fetch(`${uiApiBase}/ops/ingestion/runs?${params}`, {
      headers: getAuthHeaders()
    });
    if (!response.ok) await throwApiError(response, "Failed to list ingestion runs");
    return response.json();
  },

  async getRun(runId: string): Promise<IngestionRun> {
    const response = await fetch(`${uiApiBase}/ops/ingestion/runs/${runId}`, {
      headers: getAuthHeaders()
    });
    if (!response.ok) await throwApiError(response, "Failed to get ingestion run");
    return response.json();
  },

  async analyze(targetTables?: string[], templateId?: string): Promise<AnalyzeResponse> {
    const response = await fetch(`${uiApiBase}/ops/ingestion/analyze`, {
      method: "POST",
      headers: getAuthHeaders(),
      body: JSON.stringify({ target_tables: targetTables, template_id: templateId })
    });
    if (!response.ok) await throwApiError(response, "Analysis failed");
    return response.json();
  },

  async enrich(runId: string, selectedCandidates: IngestionCandidate[]): Promise<EnrichAsyncResponse> {
    const response = await fetch(`${uiApiBase}/ops/ingestion/enrich`, {
      method: "POST",
      headers: getAuthHeaders(),
      body: JSON.stringify({
        run_id: runId,
        selected_candidates: selectedCandidates
      })
    });
    if (!response.ok) await throwApiError(response, "Enrichment failed");
    return response.json();
  },

  async commit(runId: string, approvedPatterns: Suggestion[]): Promise<CommitResponse> {
    const response = await fetch(`${uiApiBase}/ops/ingestion/commit`, {
      method: "POST",
      headers: getAuthHeaders(),
      body: JSON.stringify({
        run_id: runId,
        approved_patterns: approvedPatterns
      })
    });
    if (!response.ok) await throwApiError(response, "Commit failed");
    return response.json();
  }
};

export const SynthService = {
  async generate(request: SynthGenerateRequest): Promise<SynthGenerateResponse> {
    const response = await fetch(`${uiApiBase}/ops/synth/generate`, {
      method: "POST",
      headers: getAuthHeaders(),
      body: JSON.stringify(request)
    });
    if (!response.ok) await throwApiError(response, "Failed to trigger generation");
    return response.json();
  },

  async listRuns(status?: string): Promise<SynthRunSummary[]> {
    const params = new URLSearchParams();
    if (status) params.append("status", status);
    const response = await fetch(`${uiApiBase}/ops/synth/runs?${params}`, {
      headers: getAuthHeaders()
    });
    if (!response.ok) await throwApiError(response, "Failed to list synth runs");
    return response.json();
  },

  async getRun(runId: string): Promise<SynthRun> {
    const response = await fetch(`${uiApiBase}/ops/synth/runs/${runId}`, {
      headers: getAuthHeaders()
    });
    if (!response.ok) await throwApiError(response, "Failed to get synth run");
    return response.json();
  }
};

export interface QueryTargetConfigPayload {
  provider: string;
  metadata: Record<string, unknown>;
  auth: Record<string, unknown>;
  guardrails: Record<string, unknown>;
  config_id?: string;
}

export interface QueryTargetConfigResponse {
  id: string;
  provider: string;
  metadata: Record<string, unknown>;
  auth: Record<string, unknown>;
  guardrails: Record<string, unknown>;
  status: string;
  last_tested_at?: string | null;
  last_test_status?: string | null;
  last_error_code?: string | null;
  last_error_message?: string | null;
  last_error_category?: string | null;
}

export interface QueryTargetSettingsResponse {
  active?: QueryTargetConfigResponse | null;
  pending?: QueryTargetConfigResponse | null;
}

export interface QueryTargetTestResponse {
  ok: boolean;
  error_code?: string | null;
  error_message?: string | null;
  error_category?: string | null;
}

export interface QueryTargetConfigHistoryEntry {
  id: string;
  config_id: string;
  event_type: string;
  snapshot: Record<string, unknown>;
  created_at?: string | null;
}

export async function fetchQueryTargetSettings(): Promise<QueryTargetSettingsResponse> {
  const response = await fetch(`${uiApiBase}/settings/query-target`, {
    headers: getAuthHeaders()
  });
  if (!response.ok) {
    await throwApiError(response, "Failed to fetch query-target settings");
  }
  return response.json();
}

export async function upsertQueryTargetSettings(
  payload: QueryTargetConfigPayload
): Promise<QueryTargetConfigResponse> {
  const response = await fetch(`${uiApiBase}/settings/query-target`, {
    method: "POST",
    headers: getAuthHeaders(),
    body: JSON.stringify(payload)
  });
  if (!response.ok) {
    await throwApiError(response, "Failed to save query-target settings");
  }
  return response.json();
}

export async function testQueryTargetSettings(
  payload: QueryTargetConfigPayload
): Promise<QueryTargetTestResponse> {
  const response = await fetch(`${uiApiBase}/settings/query-target/test-connection`, {
    method: "POST",
    headers: getAuthHeaders(),
    body: JSON.stringify(payload)
  });
  if (!response.ok) {
    await throwApiError(response, "Failed to test query-target settings");
  }
  return response.json();
}

export async function activateQueryTargetSettings(
  configId: string
): Promise<QueryTargetConfigResponse> {
  const response = await fetch(`${uiApiBase}/settings/query-target/activate`, {
    method: "POST",
    headers: getAuthHeaders(),
    body: JSON.stringify({ config_id: configId })
  });
  if (!response.ok) {
    await throwApiError(response, "Failed to activate query-target settings");
  }
  return response.json();
}

export async function fetchQueryTargetHistory(
  limit: number = 20
): Promise<QueryTargetConfigHistoryEntry[]> {
  const response = await fetch(`${uiApiBase}/settings/query-target/history?limit=${limit}`, {
    headers: getAuthHeaders()
  });
  if (!response.ok) {
    await throwApiError(response, "Failed to fetch query-target history");
  }
  return response.json();
}

/**
 * Run the agent with streaming progress updates.
 * Yields parsed SSE events.
 */
export async function* runAgentStream(request: AgentRunRequest): AsyncGenerator<{ event: string, data: any }, void, unknown> {
  const url = `${agentBase}/agent/run/stream`; // Using agentBase (could use uiApiBase)
  const response = await fetch(url, {
    method: "POST",
    headers: {
      ...getAuthHeaders(),
      "Content-Type": "application/json"
    },
    body: JSON.stringify(request)
  });

  if (!response.ok) {
    const text = await response.text();
    throw new Error(`Agent run failed: ${response.status} ${text}`);
  }

  if (!response.body) {
    throw new Error("No response body received from stream endpoint");
  }

  const reader = response.body.getReader();
  const decoder = new TextDecoder("utf-8");
  let buffer = "";

  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;

      buffer += decoder.decode(value, { stream: true });
      const lines = buffer.split("\n\n");
      // Keep the last partial chunk in buffer
      buffer = lines.pop() || "";

      for (const line of lines) {
        if (!line.trim()) continue;
        const linesInBlock = line.split("\n");
        let eventType = "";
        let data = null;

        for (const l of linesInBlock) {
          if (l.startsWith("event: ")) {
            eventType = l.substring(7).trim();
          } else if (l.startsWith("data: ")) {
            try {
              data = JSON.parse(l.substring(6));
            } catch (e) {
              console.warn("Failed to parse SSE data:", l);
            }
          }
        }

        if (eventType && data !== null) {
          yield { event: eventType, data };
        }
      }
    }
  } finally {
    reader.releaseLock();
  }
}

/**
 * Fetch operator-safe runtime diagnostics.
 */
export function getDiagnostics(debug?: boolean): Promise<DiagnosticsResponse>;
export function getDiagnostics(debug: boolean, runId: string): Promise<RunDiagnosticsResponse>;
export async function getDiagnostics(
  debug = false,
  runId?: string
): Promise<DiagnosticsResponse | RunDiagnosticsResponse> {
  const url = new URL("/agent/diagnostics", agentBase);
  if (debug) url.searchParams.set("debug", "true");
  if (runId) url.searchParams.set("audit_run_id", runId);

  const response = await fetch(url.toString(), {
    headers: {
      "Content-Type": "application/json",
      ...(internalAuthToken ? { "X-Internal-Auth": internalAuthToken } : {}),
    },
  });

  if (!response.ok) {
    const errorData = await response.json().catch(() => ({}));
    throw new ApiError(
      errorData.error?.message || "Failed to fetch diagnostics",
      response.status,
      errorData.error?.code || "DIAGNOSTICS_ERROR",
      errorData.error?.details || {}
    );
  }

  const data = await response.json();
  const isValid = runId ? isRunDiagnosticsResponse(data) : isDiagnosticsResponse(data);

  if (!isValid) {
    const report = buildContractMismatchReport("Diagnostics.getDiagnostics", data, { debug, runId });
    console.error("Operator API contract mismatch (getDiagnostics)", report);
    throw new ApiError(
      `Received unexpected response from getDiagnostics${report.ids.trace_id ? ` (trace_id=${report.ids.trace_id})` : ""}`,
      200,
      "MALFORMED_RESPONSE",
      { endpoint: "getDiagnostics", surface: report.surface, ...report.ids, request_context: report.request_context }
    );
  }
  return data;
}
