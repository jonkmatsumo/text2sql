import { components as AgentComponents } from "./gen/agent_types";
import { components as OtelComponents } from "./gen/otel_types";
import { components as UIComponents } from "./gen/ui_types";

export type AgentRunRequest = AgentComponents["schemas"]["AgentRunRequest"];
export type AgentRunResponse = AgentComponents["schemas"]["AgentRunResponse"];

export type FeedbackRequest = UIComponents["schemas"]["FeedbackRequest"];

export type TraceDetail = OtelComponents["schemas"]["TraceDetail"];
export type SpanSummary = OtelComponents["schemas"]["SpanSummary"];
export type SpanDetail = OtelComponents["schemas"]["SpanDetail"];
export type TraceSummary = OtelComponents["schemas"]["TraceSummary"];
export type PaginatedTracesResponse = OtelComponents["schemas"]["PaginatedTracesResponse"];

export interface ListTracesParams {
    service?: string;
    trace_id?: string;
    start_time_gte?: string;
    start_time_lte?: string;
    duration_min_ms?: number;
    duration_max_ms?: number;
    limit?: number;
    offset?: number;
    order?: "asc" | "desc";
}

export interface TraceAggregationsResponse {
    total_count: number;
    facet_counts: {
        service: Record<string, number>;
        status: Record<string, number>;
        error: Record<string, number>;
    };
    duration_histogram: Array<{ start_ms: number; end_ms: number; count: number }>;
    percentiles: {
        p50_ms?: number | null;
        p95_ms?: number | null;
        p99_ms?: number | null;
    };
    sampling: { is_sampled: boolean; sample_rate?: number | null };
    truncation: { is_truncated: boolean; limit?: number | null };
    as_of: string;
    window_start?: string | null;
    window_end?: string | null;
}

export type MetricsBucket = OtelComponents["schemas"]["MetricsBucket"];
export type MetricsSummary = OtelComponents["schemas"]["MetricsSummary"];
export type MetricsPreviewResponse = OtelComponents["schemas"]["MetricsPreviewResponse"];

export interface AgentProgressData {
    phase: string;
    timestamp: number;
}

export type AgentStreamEventType = "startup" | "progress" | "result" | "error";

export interface AgentStreamEvent {
    event: AgentStreamEventType;
    data: any; // Flexible data based on event type
}
