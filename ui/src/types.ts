export interface AgentRunRequest {
    question: string;
    tenantId: number;
    threadId: string;
    llmProvider?: string;
    llmModel?: string;
}

export interface AgentRunResponse {
    sql?: string;
    result?: any;
    response?: string;
    error?: string;
    from_cache: boolean;
    interaction_id?: string;
    trace_id?: string;
    viz_spec?: any;
}

export interface FeedbackRequest {
    interactionId: string;
    thumb: "UP" | "DOWN";
    comment?: string;
}

export interface TraceDetail {
    trace_id: string;
    service_name: string;
    start_time: string;
    end_time: string;
    duration_ms: number;
    span_count: number;
    status: string;
    raw_blob_url?: string | null;
    resource_attributes?: Record<string, any> | null;
    trace_attributes?: Record<string, any> | null;
    total_tokens?: number | null;
    prompt_tokens?: number | null;
    completion_tokens?: number | null;
    model_name?: string | null;
    estimated_cost_usd?: number | null;
}

export interface SpanSummary {
    span_id: string;
    trace_id: string;
    parent_span_id?: string | null;
    name: string;
    kind: string;
    status_code: string;
    status_message?: string | null;
    start_time: string;
    end_time: string;
    duration_ms: number;
    span_attributes?: Record<string, any> | null;
    events?: Array<Record<string, any>> | null;
}

export interface SpanDetail extends SpanSummary {
    links?: Array<Record<string, any>> | null;
    payloads?: Array<Record<string, any>> | null;
}

/** Summary representation of a trace for list endpoints. */
export interface TraceSummary {
    trace_id: string;
    service_name: string;
    start_time: string;
    end_time: string;
    duration_ms: number;
    span_count: number;
    status: string;
    raw_blob_url?: string | null;
}

/** Paginated response from list traces endpoint. */
export interface PaginatedTracesResponse {
    items: TraceSummary[];
    total: number;
    next_offset?: number | null;
}

/** Query parameters for list traces endpoint. */
export interface ListTracesParams {
    service?: string;
    trace_id?: string;
    start_time_gte?: string;
    start_time_lte?: string;
    limit?: number;
    offset?: number;
    order?: "asc" | "desc";
}
