import type { DiagnosticsResponse } from "../types/diagnostics";

export type DiagnosticsStatus = "healthy" | "degraded" | "unknown";

export interface DiagnosticsAnomaly {
  id: string;
  label: string;
  value: number;
  threshold: number;
}

export const DIAGNOSTICS_THRESHOLDS = {
  avgQueryComplexityWarn: 10,
  schemaCacheSizeWarn: 5000,
  truncationEventsWarn: 5,
  stageLatencyMsWarn: 2000,
} as const;

export function getDiagnosticsAnomalies(
  data: DiagnosticsResponse | null | undefined
): DiagnosticsAnomaly[] {
  if (!data) return [];

  const anomalies: DiagnosticsAnomaly[] = [];
  const runtime = data.runtime_indicators;

  if (runtime) {
    if (
      typeof runtime.avg_query_complexity === "number" &&
      runtime.avg_query_complexity > DIAGNOSTICS_THRESHOLDS.avgQueryComplexityWarn
    ) {
      anomalies.push({
        id: "avg_query_complexity",
        label: "Avg Query Complexity",
        value: runtime.avg_query_complexity,
        threshold: DIAGNOSTICS_THRESHOLDS.avgQueryComplexityWarn,
      });
    }

    if (
      typeof runtime.active_schema_cache_size === "number" &&
      runtime.active_schema_cache_size > DIAGNOSTICS_THRESHOLDS.schemaCacheSizeWarn
    ) {
      anomalies.push({
        id: "active_schema_cache_size",
        label: "Schema Cache Size",
        value: runtime.active_schema_cache_size,
        threshold: DIAGNOSTICS_THRESHOLDS.schemaCacheSizeWarn,
      });
    }

    if (
      typeof runtime.recent_truncation_event_count === "number" &&
      runtime.recent_truncation_event_count > DIAGNOSTICS_THRESHOLDS.truncationEventsWarn
    ) {
      anomalies.push({
        id: "recent_truncation_event_count",
        label: "Truncation Events (Recent)",
        value: runtime.recent_truncation_event_count,
        threshold: DIAGNOSTICS_THRESHOLDS.truncationEventsWarn,
      });
    }
  }

  if (data.debug?.latency_breakdown_ms) {
    for (const [stage, rawMs] of Object.entries(data.debug.latency_breakdown_ms)) {
      if (
        typeof rawMs === "number" &&
        rawMs > DIAGNOSTICS_THRESHOLDS.stageLatencyMsWarn
      ) {
        anomalies.push({
          id: `latency:${stage}`,
          label: `Latency (${stage})`,
          value: rawMs,
          threshold: DIAGNOSTICS_THRESHOLDS.stageLatencyMsWarn,
        });
      }
    }
  }

  return anomalies;
}

export function getDiagnosticsStatus(
  data: DiagnosticsResponse | null | undefined
): DiagnosticsStatus {
  if (!data?.runtime_indicators) return "unknown";
  return getDiagnosticsAnomalies(data).length > 0 ? "degraded" : "healthy";
}
