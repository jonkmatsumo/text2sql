import { describe, expect, it } from "vitest";
import {
  DIAGNOSTICS_THRESHOLDS,
  getDiagnosticsAnomalies,
  getDiagnosticsStatus,
} from "../diagnosticsStatus";

const baseDiagnostics = {
  diagnostics_schema_version: 1,
  active_database_provider: "postgres",
  retry_policy: { mode: "exponential", max_retries: 3 },
  schema_cache_ttl_seconds: 300,
  runtime_indicators: {
    active_schema_cache_size: 32,
    last_schema_refresh_timestamp: 1700000000,
    avg_query_complexity: 2.4,
    recent_truncation_event_count: 0,
  },
  enabled_flags: {
    schema_binding_validation: true,
    schema_binding_soft_mode: false,
    column_allowlist_mode: "strict",
    column_allowlist_from_schema_context: true,
    cartesian_join_mode: "warn",
    capability_fallback_mode: "safe",
    provider_cap_mitigation: "enabled",
    decision_summary_debug: false,
    disable_prefetch: false,
    disable_schema_refresh: false,
    disable_llm_retries: false,
  },
};

describe("diagnosticsStatus", () => {
  it("returns healthy when runtime indicators are below thresholds", () => {
    expect(getDiagnosticsStatus(baseDiagnostics as any)).toBe("healthy");
    expect(getDiagnosticsAnomalies(baseDiagnostics as any)).toEqual([]);
  });

  it("returns unknown when runtime indicators are missing", () => {
    expect(getDiagnosticsStatus(null)).toBe("unknown");
    expect(
      getDiagnosticsStatus({
        ...baseDiagnostics,
        runtime_indicators: undefined,
      } as any)
    ).toBe("unknown");
  });

  it("returns degraded and reports anomalies above thresholds", () => {
    const degraded = {
      ...baseDiagnostics,
      runtime_indicators: {
        ...baseDiagnostics.runtime_indicators,
        avg_query_complexity: DIAGNOSTICS_THRESHOLDS.avgQueryComplexityWarn + 0.1,
        active_schema_cache_size: DIAGNOSTICS_THRESHOLDS.schemaCacheSizeWarn + 1,
        recent_truncation_event_count: DIAGNOSTICS_THRESHOLDS.truncationEventsWarn + 1,
      },
      debug: {
        latency_breakdown_ms: {
          execute: DIAGNOSTICS_THRESHOLDS.stageLatencyMsWarn + 25,
          plan: 150,
        },
      },
    };

    expect(getDiagnosticsStatus(degraded as any)).toBe("degraded");
    const ids = getDiagnosticsAnomalies(degraded as any).map((item) => item.id).sort();
    expect(ids).toEqual([
      "active_schema_cache_size",
      "avg_query_complexity",
      "latency:execute",
      "recent_truncation_event_count",
    ]);
  });
});
