import {
  AgentRunRequest,
  AgentRunResponse,
  FeedbackRequest,
  SpanDetail,
  SpanSummary,
  TraceDetail,
  ListTracesParams,
  PaginatedTracesResponse,
  MetricsPreviewResponse
} from "./types";
import {
  Interaction,
  ApprovedExample,
  PinRule,
  RecommendationResult,
  PatternGenerationResult,
  PatternReloadResult
} from "./types/admin";
import {
  agentServiceBaseUrl,
  uiApiBaseUrl,
  otelWorkerBaseUrl,
  internalAuthToken
} from "./config";

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
    throw new Error(`Metrics fetch failed (${response.status})`);
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
    throw new Error(`Agent service error (${response.status})`);
  }

  return response.json();
}

export async function submitFeedback(request: FeedbackRequest): Promise<any> {
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
    throw new Error(`Trace fetch failed (${response.status})`);
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
    throw new Error(`Span fetch failed (${response.status})`);
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
    throw new Error(`Span detail fetch failed (${response.status})`);
  }
  return response.json();
}

export async function resolveTraceByInteraction(interactionId: string): Promise<string> {
  const response = await fetch(`${otelBase}/api/v1/traces/by-interaction/${interactionId}`);
  if (!response.ok) {
    throw new Error(`Trace resolve failed (${response.status})`);
  }
  const data = await response.json();
  return data.trace_id;
}

export async function listTraces(
  params: ListTracesParams = {}
): Promise<PaginatedTracesResponse> {
  const searchParams = new URLSearchParams();
  if (params.service) searchParams.append("service", params.service);
  if (params.trace_id) searchParams.append("trace_id", params.trace_id);
  if (params.start_time_gte) searchParams.append("start_time_gte", params.start_time_gte);
  if (params.start_time_lte) searchParams.append("start_time_lte", params.start_time_lte);
  if (params.limit !== undefined) searchParams.append("limit", params.limit.toString());
  if (params.offset !== undefined) searchParams.append("offset", params.offset.toString());
  if (params.order) searchParams.append("order", params.order);

  const url = `${otelBase}/api/v1/traces${searchParams.toString() ? `?${searchParams}` : ""}`;
  const response = await fetch(url);
  if (!response.ok) {
    throw new Error(`List traces failed (${response.status})`);
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

  async publishApproved(limit: number = 50): Promise<any> {
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

  async hydrateSchema(): Promise<any> {
    const response = await fetch(`${uiApiBase}/ops/schema-hydrate`, {
      method: "POST",
      headers: getAuthHeaders()
    });
    if (!response.ok) await throwApiError(response, "Schema hydration failed");
    return response.json();
  },

  async reindexCache(): Promise<any> {
    const response = await fetch(`${uiApiBase}/ops/semantic-cache/reindex`, {
      method: "POST",
      headers: getAuthHeaders()
    });
    if (!response.ok) await throwApiError(response, "Cache re-indexing failed");
    return response.json();
  },

  async getJobStatus(jobId: string): Promise<any> {
    const response = await fetch(`${uiApiBase}/ops/jobs/${jobId}`, {
      headers: getAuthHeaders()
    });
    if (!response.ok) await throwApiError(response, "Failed to fetch job status");
    return response.json();
  }
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

export interface EnrichResponse {
  suggestions: Suggestion[];
}

export interface CommitResponse {
  inserted_count: number;
  hydration_job_id: string;
}

export const IngestionService = {
  async analyze(targetTables?: string[]): Promise<AnalyzeResponse> {
    const response = await fetch(`${uiApiBase}/ops/ingestion/analyze`, {
      method: "POST",
      headers: getAuthHeaders(),
      body: JSON.stringify({ target_tables: targetTables })
    });
    if (!response.ok) await throwApiError(response, "Analysis failed");
    return response.json();
  },

  async enrich(runId: string, selectedCandidates: IngestionCandidate[]): Promise<EnrichResponse> {
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
