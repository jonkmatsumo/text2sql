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
    limit?: number;
    offset?: number;
    order?: "asc" | "desc";
}

export type MetricsBucket = OtelComponents["schemas"]["MetricsBucket"];
export type MetricsSummary = OtelComponents["schemas"]["MetricsSummary"];
export type MetricsPreviewResponse = OtelComponents["schemas"]["MetricsPreviewResponse"];
