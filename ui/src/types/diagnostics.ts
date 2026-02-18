export interface RetryPolicy {
    mode: string;
    max_retries: number;
}

export interface RuntimeIndicators {
    active_schema_cache_size: number;
    last_schema_refresh_timestamp: number | null;
    avg_query_complexity: number;
    recent_truncation_event_count: number;
}

export interface EnabledFlags extends Record<string, boolean | string> {
    schema_binding_validation: boolean;
    schema_binding_soft_mode: boolean;
    column_allowlist_mode: string;
    column_allowlist_from_schema_context: boolean;
    cartesian_join_mode: string;
    capability_fallback_mode: string;
    provider_cap_mitigation: string;
    decision_summary_debug: boolean;
    disable_prefetch: boolean;
    disable_schema_refresh: boolean;
    disable_llm_retries: boolean;
}

export interface DiagnosticsDebug {
    latency_breakdown_ms: Record<string, number>;
    trace_id?: string;
    interaction_id?: string;
    request_id?: string;
}

export interface DiagnosticsResponse {
    diagnostics_schema_version: number;
    active_database_provider?: string;
    trace_id?: string;
    interaction_id?: string;
    request_id?: string;
    retry_policy?: RetryPolicy;
    schema_cache_ttl_seconds?: number;
    runtime_indicators?: RuntimeIndicators;
    enabled_flags: EnabledFlags;
    monitor_snapshot?: Record<string, any>;
    run_summary_store?: Record<string, any>;
    audit_events?: Array<Record<string, any>>;
    debug?: DiagnosticsDebug;
    self_test?: Record<string, any>;
}

export interface RunContextDiagnostics {
    user_nlq_text?: string;
    execution_status?: string;
    created_at?: string;
    [key: string]: unknown;
}

export interface ValidationDiagnostics {
    ast_valid?: boolean;
    syntax_errors?: string[];
    [key: string]: unknown;
}

export interface CompletenessDiagnostics {
    is_truncated?: boolean;
    [key: string]: unknown;
}

export interface RunDiagnosticsResponse extends DiagnosticsResponse {
    run_context?: RunContextDiagnostics;
    validation?: ValidationDiagnostics;
    completeness?: CompletenessDiagnostics;
    generated_sql?: string;
    audit_events?: Array<Record<string, unknown>>;
}
