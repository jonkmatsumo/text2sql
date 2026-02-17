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

export interface EnabledFlags {
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

export interface DiagnosticsResponse {
    diagnostics_schema_version: number;
    active_database_provider?: string;
    retry_policy: RetryPolicy;
    schema_cache_ttl_seconds: number;
    runtime_indicators: RuntimeIndicators;
    enabled_flags: EnabledFlags;
    monitor_snapshot?: Record<string, any>;
    run_summary_store?: Record<string, any>;
    audit_events?: Array<Record<string, any>>;
    debug?: {
        latency_breakdown_ms: Record<string, number>;
    };
    self_test?: Record<string, any>;
}
