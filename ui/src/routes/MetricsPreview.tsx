import React, { useEffect, useMemo, useState } from "react";
import { Link } from "react-router-dom";
import { VegaEmbed } from "react-vega";
import { listTraces } from "../api";
import { TraceSummary } from "../types";
import { grafanaBaseUrl } from "../config";

interface TimeSeriesPoint {
  timestamp: Date;
  value: number;
}

interface MetricPanel {
  title: string;
  description: string;
  data: TimeSeriesPoint[];
  color: string;
  unit?: string;
}

function aggregateByHour(
  traces: TraceSummary[],
  extractor: (trace: TraceSummary) => number
): TimeSeriesPoint[] {
  if (!traces.length) return [];

  const buckets = new Map<string, { timestamp: Date; sum: number; count: number }>();

  traces.forEach((trace) => {
    const date = new Date(trace.start_time);
    // Round to hour
    date.setMinutes(0, 0, 0);
    const key = date.toISOString();

    if (!buckets.has(key)) {
      buckets.set(key, { timestamp: date, sum: 0, count: 0 });
    }
    const bucket = buckets.get(key)!;
    bucket.sum += extractor(trace);
    bucket.count += 1;
  });

  return Array.from(buckets.values())
    .map((b) => ({ timestamp: b.timestamp, value: b.count }))
    .sort((a, b) => a.timestamp.getTime() - b.timestamp.getTime());
}

function aggregateErrorRateByHour(traces: TraceSummary[]): TimeSeriesPoint[] {
  if (!traces.length) return [];

  const buckets = new Map<string, { timestamp: Date; errors: number; total: number }>();

  traces.forEach((trace) => {
    const date = new Date(trace.start_time);
    date.setMinutes(0, 0, 0);
    const key = date.toISOString();

    if (!buckets.has(key)) {
      buckets.set(key, { timestamp: date, errors: 0, total: 0 });
    }
    const bucket = buckets.get(key)!;
    bucket.total += 1;
    if (trace.status.toLowerCase() === "error") {
      bucket.errors += 1;
    }
  });

  return Array.from(buckets.values())
    .map((b) => ({
      timestamp: b.timestamp,
      value: b.total > 0 ? (b.errors / b.total) * 100 : 0
    }))
    .sort((a, b) => a.timestamp.getTime() - b.timestamp.getTime());
}

function TimeSeriesChart({
  data,
  color,
  unit
}: {
  data: TimeSeriesPoint[];
  color: string;
  unit?: string;
}) {
  if (data.length === 0) {
    return (
      <div
        style={{
          padding: "40px",
          textAlign: "center",
          color: "var(--muted)",
          backgroundColor: "var(--surface-muted)",
          borderRadius: "8px"
        }}
      >
        No data available
      </div>
    );
  }

  const spec: any = {
    $schema: "https://vega.github.io/schema/vega-lite/v5.json",
    width: "container",
    height: 200,
    data: {
      values: data.map((d) => ({
        timestamp: d.timestamp.toISOString(),
        value: d.value
      }))
    },
    mark: {
      type: "area",
      line: { color },
      color: {
        x1: 1,
        y1: 1,
        x2: 1,
        y2: 0,
        gradient: "linear",
        stops: [
          { offset: 0, color: "transparent" },
          { offset: 1, color: `${color}40` }
        ]
      }
    },
    encoding: {
      x: {
        field: "timestamp",
        type: "temporal",
        axis: { title: null, format: "%m/%d %H:%M" }
      },
      y: {
        field: "value",
        type: "quantitative",
        axis: { title: unit || null }
      },
      tooltip: [
        { field: "timestamp", type: "temporal", title: "Time" },
        { field: "value", type: "quantitative", title: unit || "Value", format: ".2f" }
      ]
    },
    config: {
      view: { stroke: null },
      axis: { grid: true, gridColor: "#e5e7eb" }
    }
  };

  return <VegaEmbed spec={spec} options={{ actions: false }} />;
}

function MetricCard({ panel }: { panel: MetricPanel }) {
  return (
    <div className="panel" style={{ marginBottom: "24px" }}>
      <div style={{ marginBottom: "16px" }}>
        <h3 style={{ margin: "0 0 4px 0" }}>{panel.title}</h3>
        <p style={{ margin: 0, color: "var(--muted)", fontSize: "0.9rem" }}>
          {panel.description}
        </p>
      </div>
      <TimeSeriesChart data={panel.data} color={panel.color} unit={panel.unit} />
    </div>
  );
}

export default function MetricsPreview() {
  const [traces, setTraces] = useState<TraceSummary[]>([]);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const loadTraces = async () => {
      setIsLoading(true);
      setError(null);

      try {
        // Load recent traces to compute metrics
        const result = await listTraces({
          limit: 500,
          order: "desc"
        });
        setTraces(result.items);
      } catch (err: any) {
        setError(err.message || "Failed to load metrics data");
      } finally {
        setIsLoading(false);
      }
    };

    loadTraces();
  }, []);

  const panels: MetricPanel[] = useMemo(() => {
    if (!traces.length) return [];

    return [
      {
        title: "Traces Over Time",
        description: "Number of traces recorded per hour",
        data: aggregateByHour(traces, () => 1),
        color: "#6366f1",
        unit: "traces"
      },
      {
        title: "Error Rate Over Time",
        description: "Percentage of traces with error status per hour",
        data: aggregateErrorRateByHour(traces),
        color: "#ef4444",
        unit: "% errors"
      },
      {
        title: "Average Duration Over Time",
        description: "Average trace duration in milliseconds per hour",
        data: (() => {
          const buckets = new Map<string, { timestamp: Date; sum: number; count: number }>();
          traces.forEach((trace) => {
            const date = new Date(trace.start_time);
            date.setMinutes(0, 0, 0);
            const key = date.toISOString();
            if (!buckets.has(key)) {
              buckets.set(key, { timestamp: date, sum: 0, count: 0 });
            }
            const bucket = buckets.get(key)!;
            bucket.sum += trace.duration_ms;
            bucket.count += 1;
          });
          return Array.from(buckets.values())
            .map((b) => ({
              timestamp: b.timestamp,
              value: b.count > 0 ? b.sum / b.count : 0
            }))
            .sort((a, b) => a.timestamp.getTime() - b.timestamp.getTime());
        })(),
        color: "#10b981",
        unit: "ms"
      }
    ];
  }, [traces]);

  const timeWindow = useMemo(() => {
    if (!traces.length) return null;
    const timestamps = traces.map(t => new Date(t.start_time).getTime());
    const min = new Date(Math.min(...timestamps));
    const max = new Date(Math.max(...timestamps));
    return { min, max };
  }, [traces]);

  const isTruncated = traces.length >= 500;
  const hasGrafana = !!grafanaBaseUrl;

  return (
    <>
      <header className="hero">
        <div>
          <p className="kicker">Observability</p>
          <h1>Metrics (Preview)</h1>
          <p className="subtitle">
            Basic metrics derived from the <strong>latest 500 traces</strong>. For advanced dashboards, use Grafana.
          </p>
        </div>
      </header>

      <div style={{ marginBottom: "24px" }}>
        <div
          style={{
            padding: "16px 20px",
            backgroundColor: "rgba(99, 102, 241, 0.1)",
            border: "1px solid rgba(99, 102, 241, 0.2)",
            borderRadius: "8px",
            display: "flex",
            alignItems: "center",
            justifyContent: "space-between",
            gap: "16px"
          }}
        >
          <div>
            <div style={{ fontWeight: 500, marginBottom: "4px" }}>
              Preview Mode
            </div>
            <div style={{ color: "var(--muted)", fontSize: "0.9rem" }}>
              These metrics are computed client-side from the most recent 500 traces.
              For production monitoring with alerting and longer retention, please configure Grafana dashboards.
            </div>
          </div>
          {hasGrafana && (
            <a
              href={grafanaBaseUrl}
              target="_blank"
              rel="noopener noreferrer"
              style={{
                padding: "10px 20px",
                borderRadius: "8px",
                border: "1px solid var(--accent)",
                backgroundColor: "transparent",
                color: "var(--accent)",
                fontWeight: 500,
                textDecoration: "none",
                whiteSpace: "nowrap"
              }}
            >
              Open Grafana Dashboards
            </a>
          )}
        </div>
      </div>

      {isLoading && (
        <div className="panel" style={{ textAlign: "center", padding: "60px" }}>
          <div style={{ color: "var(--muted)" }}>Loading metrics data...</div>
        </div>
      )}

      {error && (
        <div className="panel" style={{ textAlign: "center", padding: "40px" }}>
          <div style={{ color: "var(--error)", marginBottom: "16px" }}>
            Failed to load metrics data
          </div>
          <div style={{ color: "var(--muted)", marginBottom: "20px", fontSize: "0.9rem" }}>
            {error}
          </div>
          <button
            type="button"
            onClick={() => window.location.reload()}
            style={{
              padding: "10px 20px",
              borderRadius: "8px",
              border: "none",
              backgroundColor: "var(--accent)",
              color: "#fff",
              fontWeight: 500,
              cursor: "pointer"
            }}
          >
            Retry
          </button>
        </div>
      )}

      {!isLoading && !error && traces.length === 0 && (
        <div className="panel" style={{ textAlign: "center", padding: "60px" }}>
          <div style={{ fontSize: "1.2rem", marginBottom: "12px" }}>
            No trace data available
          </div>
          <div style={{ color: "var(--muted)", marginBottom: "20px" }}>
            Metrics will appear once traces are recorded in the telemetry store.
          </div>
          <Link
            to="/admin/traces/search"
            style={{
              padding: "10px 20px",
              borderRadius: "8px",
              border: "1px solid var(--border)",
              backgroundColor: "transparent",
              color: "var(--ink)",
              fontWeight: 500,
              textDecoration: "none",
              display: "inline-block"
            }}
          >
            Go to Trace Search
          </Link>
        </div>
      )}

      {!isLoading && !error && panels.length > 0 && (
        <div>
          <div
            style={{
              display: "grid",
              gridTemplateColumns: "repeat(auto-fit, minmax(400px, 1fr))",
              gap: "24px"
            }}
          >
            {panels.map((panel) => (
              <MetricCard key={panel.title} panel={panel} />
            ))}
          </div>

          <div style={{ marginTop: "32px", padding: "16px", backgroundColor: "var(--surface-muted)", borderRadius: "8px", border: "1px solid var(--border)" }}>
            <div style={{ fontWeight: 600, marginBottom: "8px" }}>Data Scope</div>
            <div style={{ fontSize: "0.9rem", color: "var(--ink)", display: "grid", gap: "4px" }}>
                <div>
                    <strong>Sample Size:</strong> {traces.length} traces {isTruncated && <span style={{color: "var(--error)", fontWeight: 500}}>(Truncated limit)</span>}
                </div>
                {timeWindow && (
                    <div>
                        <strong>Time Window:</strong> {timeWindow.min.toLocaleString()} — {timeWindow.max.toLocaleString()}
                    </div>
                )}
                <div style={{ color: "var(--muted)", marginTop: "4px", fontStyle: "italic" }}>
                    * These metrics are calculated client-side from the most recent traces. Older data is not included.
                </div>
            </div>
          </div>
        </div>
      )}

      <div style={{ marginTop: "32px", textAlign: "center" }}>
        <Link
          to="/admin/operations"
          style={{ color: "var(--accent)", textDecoration: "none" }}
        >
          Back to System Operations
        </Link>
      </div>
    </>
  );
}
