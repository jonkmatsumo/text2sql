import { components as UIComponents } from "../gen/ui_types";

export type InteractionStatus = "PENDING" | "APPROVED" | "REJECTED" | "UNKNOWN" | "SUCCESS" | "FAILED";
export type FeedbackThumb = "UP" | "DOWN" | "None" | "All";

export interface Interaction {
    id: string;
    user_nlq_text: string;
    generated_sql: string;
    generated_sql_preview: string;
    response_payload: string;
    execution_status: InteractionStatus;
    thumb?: FeedbackThumb;
    trace_id?: string;
    created_at: string;
    model_version?: string;
    tables_used?: string[];
    feedback?: Array<{
        thumb: FeedbackThumb;
        comment?: string;
    }>;
}

export interface ApprovedExample {
    id?: string;
    question: string;
    sql_query: string;
    status: string;
    created_at: string;
}

export type PinRule = UIComponents["schemas"]["PinRuleUpsertRequest"] & { id: string };

export interface RecommendationMetadata {
    count_total: number;
    count_approved: number;
    count_seeded: number;
    count_fallback: number;
    pins_selected_count: number;
    pins_matched_rules: string[];
    truncated: boolean;
}

export interface RecommendationResult {
    examples: Array<{
        question: string;
        source: string;
        metadata: {
            fingerprint: string;
            status: string;
            pinned?: boolean;
        };
    }>;
    metadata: RecommendationMetadata;
    fallback_used: boolean;
}

export interface PatternGenerationMetrics {
    generated_count: number;
    created_count: number;
    updated_count: number;
}

export interface PatternGenerationResult {
    success: boolean;
    run_id?: string;
    metrics?: PatternGenerationMetrics;
    error?: string;
}

export interface PatternReloadResult {
    success: boolean;
    message: string;
    reload_id?: string;
    duration_ms?: number;
    pattern_count?: number;
    error?: string;
}

export type OpsJobStatus = "PENDING" | "RUNNING" | "CANCELLING" | "CANCELLED" | "COMPLETED" | "FAILED";

export interface OpsJobResponse {
    id: string;
    job_type: string;
    status: OpsJobStatus;
    started_at: string;
    finished_at?: string | null;
    error_message?: string | null;
    result?: Record<string, any>;
}

export interface JobStatusResponse {
    id: string;
    job_type: string;
    status: OpsJobStatus;
    started_at: string;
    finished_at?: string | null;
    error_message?: string | null;
    result?: Record<string, unknown>;
}

export interface SynthRunSummary {
    id: string;
    started_at: string;
    completed_at?: string;
    status: string;
    job_id?: string;
}

export interface SynthRun {
    id: string;
    started_at: string;
    completed_at?: string;
    status: string;
    config_snapshot: Record<string, any>;
    output_path?: string;
    manifest?: any;
    metrics: Record<string, any>;
    error_message?: string;
    job_id?: string;
}

export interface SynthGenerateRequest {
    preset?: string;
    config?: Record<string, any>;
    output_path?: string;
    only?: string[];
}

export interface SynthGenerateResponse {
    run_id: string;
    job_id: string;
}

export interface ListRunsResponse {
    runs: Interaction[];
    has_more?: boolean;
    total_count?: number;
}

/**
 * @deprecated Use ListRunsResponse.
 */
export interface InteractionListResponse {
    data: Interaction[];
    has_more?: boolean;
    total_count?: number;
}
