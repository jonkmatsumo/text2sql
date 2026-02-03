import React, { useMemo } from "react";
import { TraceSummary } from "../../../types";

interface DurationHistogramProps {
  traces: TraceSummary[];
  bins: Array<{ start_ms: number; end_ms: number; count: number }> | null;
  scopeLabel: string;
  range: { min: number | null; max: number | null };
  onRangeChange: (next: { min: number | null; max: number | null }) => void;
}

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

export const DurationHistogram: React.FC<DurationHistogramProps> = ({
  traces,
  bins,
  scopeLabel,
  range,
  onRangeChange
}) => {
  const effectiveBins = useMemo(
    () => bins ?? computeClientBins(traces),
    [bins, traces]
  );

  const maxCount = Math.max(1, ...effectiveBins.map((b) => b.count));
  const durations = useMemo(() => traces.map((t) => t.duration_ms), [traces]);
  const p50 = percentile(durations, 50);
  const p95 = percentile(durations, 95);
  const p99 = percentile(durations, 99);

  return (
    <div className="trace-histogram">
      <div className="trace-histogram__header">
        <div>
          <h3>Duration Distribution</h3>
          <p className="subtitle">{scopeLabel}</p>
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
        </div>
      )}

      <div className="trace-histogram__percentiles">
        <span>P50: {p50 != null ? `${Math.round(p50)} ms` : "—"}</span>
        <span>P95: {p95 != null ? `${Math.round(p95)} ms` : "—"}</span>
        <span>P99: {p99 != null ? `${Math.round(p99)} ms` : "—"}</span>
      </div>
    </div>
  );
};
