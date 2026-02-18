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

/**
 * Normalizes a metric value:
 * - Reject non-finite (NaN, Infinity)
 * - Clamp negatives to 0
 * - Return null for unusable values (not numbers)
 */
export function normalizeNonNegativeMetric(value: unknown): number | null {
  if (typeof value !== "number" || !Number.isFinite(value)) {
    return null;
  }
  return value < 0 ? 0 : value;
}

export function getDiagnosticsAnomalies(
  data: DiagnosticsResponse | null | undefined
): DiagnosticsAnomaly[] {
  if (!data) return [];

  const anomalies: DiagnosticsAnomaly[] = [];
  const runtime = data.runtime_indicators;

  if (runtime) {
    const avgQueryComplexity = normalizeNonNegativeMetric(runtime.avg_query_complexity);
    if (avgQueryComplexity != null && avgQueryComplexity > DIAGNOSTICS_THRESHOLDS.avgQueryComplexityWarn) {
      anomalies.push({
        id: "avg_query_complexity",
        label: "Avg Query Complexity",
        value: avgQueryComplexity,
        threshold: DIAGNOSTICS_THRESHOLDS.avgQueryComplexityWarn,
      });
    }

    const schemaCacheSize = normalizeNonNegativeMetric(runtime.active_schema_cache_size);
    if (schemaCacheSize != null && schemaCacheSize > DIAGNOSTICS_THRESHOLDS.schemaCacheSizeWarn) {
      anomalies.push({
        id: "active_schema_cache_size",
        label: "Schema Cache Size",
        value: schemaCacheSize,
        threshold: DIAGNOSTICS_THRESHOLDS.schemaCacheSizeWarn,
      });
    }

    const truncationEvents = normalizeNonNegativeMetric(runtime.recent_truncation_event_count);
    if (truncationEvents != null && truncationEvents > DIAGNOSTICS_THRESHOLDS.truncationEventsWarn) {
      anomalies.push({
        id: "recent_truncation_event_count",
        label: "Truncation Events (Recent)",
        value: truncationEvents,
        threshold: DIAGNOSTICS_THRESHOLDS.truncationEventsWarn,
      });
    }
  }

  if (data.debug?.latency_breakdown_ms) {
    for (const [stage, rawMs] of Object.entries(data.debug.latency_breakdown_ms)) {
      const latencyMs = normalizeNonNegativeMetric(rawMs);
      if (latencyMs != null && latencyMs > DIAGNOSTICS_THRESHOLDS.stageLatencyMsWarn) {
        anomalies.push({
          id: `latency:${stage}`,
          label: `Latency (${stage})`,
          value: latencyMs,
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
