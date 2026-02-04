import React, { useMemo } from "react";
import { TraceSummary } from "../../../types";

interface DurationHistogramProps {
  traces: TraceSummary[];
  bins: Array<{ start_ms: number; end_ms: number; count: number }> | null;
  scopeLabel: string;
  range: { min: number | null; max: number | null };
  onRangeChange: (next: { min: number | null; max: number | null }) => void;
  percentiles?: { p50_ms?: number | null; p95_ms?: number | null; p99_ms?: number | null };
}

type PercentileKey = "p50_ms" | "p95_ms" | "p99_ms";

function computeClientBins(traces: TraceSummary[], bucketCount = 12) {
  if (traces.length === 0) return [];
  const durations = traces.map((t) => t.duration_ms);
  const min = Math.min(...durations);
  const max = Math.max(...durations);
  const span = Math.max(1, max - min);
  const bucketSize = Math.ceil(span / bucketCount);
  const bins = Array.from({ length: bucketCount }, (_, idx) => ({
    start_ms: min + idx * bucketSize,
    end_ms: min + (idx + 1) * bucketSize,
    count: 0
  }));
  durations.forEach((value) => {
    const idx = Math.min(bucketCount - 1, Math.floor((value - min) / bucketSize));
    bins[idx].count += 1;
  });
  return bins;
}

function percentile(values: number[], p: number) {
  if (values.length === 0) return null;
  const sorted = [...values].sort((a, b) => a - b);
  const idx = Math.ceil((p / 100) * sorted.length) - 1;
  return sorted[Math.max(0, Math.min(sorted.length - 1, idx))];
}

function derivePercentilesFromBins(
  bins: Array<{ start_ms: number; end_ms: number; count: number }>
): Partial<Record<PercentileKey, number>> {
  const total = bins.reduce((sum, bin) => sum + (bin.count ?? 0), 0);
  if (total === 0) return {};
  const thresholds: Record<PercentileKey, number> = {
    p50_ms: total * 0.5,
    p95_ms: total * 0.95,
    p99_ms: total * 0.99
  };
  const result: Partial<Record<PercentileKey, number>> = {};
  let cumulative = 0;
  bins.forEach((bin) => {
    cumulative += bin.count ?? 0;
    Object.entries(thresholds).forEach(([key, target]) => {
      const typedKey = key as PercentileKey;
      if (result[typedKey] == null && cumulative >= target) {
        result[typedKey] = bin.start_ms ?? bin.end_ms ?? 0;
      }
    });
  });
  return result;
}

export const DurationHistogram: React.FC<DurationHistogramProps> = ({
  traces,
  bins,
  scopeLabel,
  range,
  onRangeChange,
  percentiles
}) => {
  const effectiveBins = useMemo(
    () => bins ?? computeClientBins(traces),
    [bins, traces]
  );

  const durations = useMemo(() => traces.map((t) => t.duration_ms), [traces]);

  const computedPercentiles = useMemo(() => {
    const hasServerPercentiles =
      percentiles &&
      (percentiles.p50_ms != null || percentiles.p95_ms != null || percentiles.p99_ms != null);
    if (hasServerPercentiles) return percentiles!;
    if (effectiveBins.length > 0) {
      const derived = derivePercentilesFromBins(effectiveBins);
      if (Object.keys(derived).length > 0) {
        return derived;
      }
    }
    if (durations.length > 0) {
      return {
        p50_ms: percentile(durations, 50),
        p95_ms: percentile(durations, 95),
        p99_ms: percentile(durations, 99)
      };
    }
    return null;
  }, [percentiles, effectiveBins, durations]);

  const maxCount = Math.max(1, ...effectiveBins.map((b) => b.count));

  const histogramRange = useMemo(() => {
    if (effectiveBins.length > 0) {
      const first = effectiveBins[0];
      const last = effectiveBins[effectiveBins.length - 1];
      return {
        min: first.start_ms,
        max: last.end_ms
      };
    }
    if (durations.length > 0) {
      return {
        min: Math.min(...durations),
        max: Math.max(...durations)
      };
    }
    return { min: 0, max: 1 };
  }, [effectiveBins, durations]);

  const percentileOrigin = useMemo(() => {
    if (percentiles && (percentiles.p50_ms != null || percentiles.p95_ms != null || percentiles.p99_ms != null)) {
      return "server";
    }
    if (effectiveBins.length > 0) {
      return "histogram";
    }
    if (durations.length > 0) {
      return "subset";
    }
    return null;
  }, [percentiles, effectiveBins, durations]);

  const percentileMarkers = useMemo(() => {
    if (!computedPercentiles) return [];
    const entries: Array<{ label: string; value: number; isExact: boolean }> = [];
    const mapping: Array<[PercentileKey, string]> = [
      ["p50_ms", "P50"],
      ["p95_ms", "P95"],
      ["p99_ms", "P99"]
    ];
    mapping.forEach(([key, label]) => {
      const value = computedPercentiles[key];
      if (typeof value === "number") {
        entries.push({
          label,
          value,
          isExact: percentileOrigin === "server"
        });
      }
    });
    return entries;
  }, [computedPercentiles, percentileOrigin]);

  const percentileNotes: Record<string, string> = {
    server: "Exact percentiles provided by the dataset.",
    histogram: "Approximate percentiles derived from histogram buckets.",
    subset: "Percentiles based on the currently loaded spans."
  };

  const quickRangeOptions = useMemo(() => {
    const presets: Array<{ label: string; min: number | null; max: number | null }> = [
      { label: "< 100ms", min: null, max: 100 },
      { label: "< 500ms", min: null, max: 500 }
    ];
    if (computedPercentiles?.p50_ms != null && computedPercentiles?.p95_ms != null) {
      presets.push({
        label: "P50–P95",
        min: Math.round(computedPercentiles.p50_ms),
        max: Math.round(computedPercentiles.p95_ms)
      });
    }
    if (computedPercentiles?.p95_ms != null) {
      presets.push({
        label: "> P95",
        min: Math.round(computedPercentiles.p95_ms),
        max: null
      });
    }
    return presets;
  }, [computedPercentiles]);

  const span = Math.max(1, histogramRange.max - histogramRange.min);

  return (
    <div className="trace-histogram">
      <div className="trace-histogram__header">
        <div>
          <h3>Duration Distribution</h3>
          <span className="trace-histogram__scope">{scopeLabel}</span>
          {percentileOrigin && (
            <p className="trace-histogram__percentile-note">
              {percentileNotes[percentileOrigin]}
            </p>
          )}
        </div>
        <div className="trace-histogram__inputs">
          <label>
            Min (ms)
            <input
              type="number"
              value={range.min ?? ""}
              onChange={(event) =>
                onRangeChange({
                  min: event.target.value === "" ? null : Number(event.target.value),
                  max: range.max
                })
              }
            />
          </label>
          <label>
            Max (ms)
            <input
              type="number"
              value={range.max ?? ""}
              onChange={(event) =>
                onRangeChange({
                  min: range.min,
                  max: event.target.value === "" ? null : Number(event.target.value)
                })
              }
            />
          </label>
        </div>
      </div>

      {quickRangeOptions.length > 0 && (
        <div className="trace-histogram__presets">
          {quickRangeOptions.map((preset) => {
            const isActive = preset.min === range.min && preset.max === range.max;
            return (
              <button
                key={`${preset.label}-${preset.min}-${preset.max}`}
                type="button"
                className={`trace-histogram__preset${isActive ? " is-active" : ""}`}
                onClick={() => onRangeChange({ min: preset.min, max: preset.max })}
              >
                {preset.label}
              </button>
            );
          })}
        </div>
      )}

      {effectiveBins.length === 0 ? (
        <div className="trace-histogram__empty">No duration data available.</div>
      ) : (
        <div className="trace-histogram__bars">
          {effectiveBins.map((bin, idx) => (
            <div key={`${bin.start_ms}-${idx}`} className="trace-histogram__bar">
              <div
                className="trace-histogram__bar-fill"
                style={{ height: `${(bin.count / maxCount) * 100}%` }}
                title={`${bin.start_ms}–${bin.end_ms} ms (${bin.count})`}
              />
            </div>
          ))}
          <div className="trace-histogram__markers">
            {percentileMarkers.map((marker) => {
              const percentage = ((marker.value - histogramRange.min) / span) * 100;
              const left = Math.max(0, Math.min(100, percentage));
              return (
                <div
                  key={`${marker.label}-${marker.value}`}
                  className="trace-histogram__marker"
                  style={{ left: `${left}%` }}
                  title={`${marker.label} ${Math.round(marker.value)} ms (${marker.isExact ? "exact" : "approximate"})`}
                >
                  <span className="trace-histogram__marker-label">{marker.label}</span>
                  <span className="trace-histogram__marker-line" />
                </div>
              );
            })}
          </div>
        </div>
      )}

      <div className="trace-histogram__percentiles">
        <span>P50: {computedPercentiles?.p50_ms != null ? `${Math.round(computedPercentiles.p50_ms)} ms` : "—"}</span>
        <span>P95: {computedPercentiles?.p95_ms != null ? `${Math.round(computedPercentiles.p95_ms)} ms` : "—"}</span>
        <span>P99: {computedPercentiles?.p99_ms != null ? `${Math.round(computedPercentiles.p99_ms)} ms` : "—"}</span>
      </div>
    </div>
  );
};
