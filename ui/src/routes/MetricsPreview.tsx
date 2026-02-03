import React, { useEffect, useMemo, useState } from "react";
import { Link } from "react-router-dom";
import { VegaEmbed } from "react-vega";
import { fetchMetricsPreview } from "../api";
import { MetricsPreviewResponse, MetricsBucket } from "../types";
import { grafanaBaseUrl } from "../config";
import { ChartFrame } from "../components/charts/ChartFrame";

interface MetricPanel {
  title: string;
  description: string;
  data: { timestamp: string; value: number | null }[];
  color: string;
  unit?: string;
  missingBuckets?: boolean;
}

const WINDOW_OPTIONS = [
  { label: "Last 15 Minutes", value: "15m" },
  { label: "Last Hour", value: "1h" },
  { label: "Last 24 Hours", value: "24h" },
  { label: "Last 7 Days", value: "7d" }
];

function TimeSeriesChart({
  data,
  color,
  unit
}: {
  data: { timestamp: string; value: number | null }[];
  color: string;
  unit?: string;
}) {
  if (data.length === 0) return null;

  const spec: any = {
    $schema: "https://vega.github.io/schema/vega-lite/v5.json",
    width: "container",
    height: 200,
    data: {
      values: data
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

function MetricCard({
  panel,
  isLoading,
  error,
  meta
}: {
  panel: MetricPanel;
  isLoading: boolean;
  error: string | null;
  meta?: { asOf?: string; window?: string };
}) {
  const hasData = panel.data.some((point) => point.value != null);
  const isEmpty = !isLoading && !error && !hasData;

  return (
    <ChartFrame
      title={panel.title}
      description={panel.description}
      isLoading={isLoading}
      error={error}
      isEmpty={isEmpty}
      meta={{
        ...meta,
        missingBuckets: panel.missingBuckets
      }}
    >
      <TimeSeriesChart data={panel.data} color={panel.color} unit={panel.unit} />
    </ChartFrame>
  );
}

export default function MetricsPreview() {
  const [metrics, setMetrics] = useState<MetricsPreviewResponse | null>(null);
  const [window, setWindow] = useState("1h");
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [asOfLabel, setAsOfLabel] = useState<string | null>(null);

  const loadMetrics = async (currentWindow: string) => {
    setIsLoading(true);
    setError(null);

    try {
      const data = await fetchMetricsPreview(currentWindow);
      setMetrics(data);
      setAsOfLabel(`(client) ${new Date().toLocaleString()}`);
    } catch (err: any) {
      setError(err.message || "Failed to load metrics data");
    } finally {
      setIsLoading(false);
    }
  };

  useEffect(() => {
    loadMetrics(window);
  }, [window]);

  const panels: MetricPanel[] = useMemo(() => {
    if (!metrics || !metrics.timeseries) {
      return [
        {
          title: "Traces Over Time",
          description: "Number of traces recorded",
          data: [],
          color: "#6366f1",
          unit: "traces",
          missingBuckets: false
        },
        {
          title: "Error Rate Over Time",
          description: "Percentage of traces with error status",
          data: [],
          color: "#ef4444",
          unit: "% errors",
          missingBuckets: false
        },
        {
          title: "Average Duration Over Time",
          description: "Average trace duration in milliseconds",
          data: [],
          color: "#10b981",
          unit: "ms",
          missingBuckets: false
        }
      ];
    }

    return [
      {
        title: "Traces Over Time",
        description: "Number of traces recorded",
        data: metrics.timeseries.map((b: MetricsBucket) => ({
          timestamp: b.timestamp,
          value: b.count
        })),
        color: "#6366f1",
        unit: "traces",
        missingBuckets: false
      },
      {
        title: "Error Rate Over Time",
        description: "Percentage of traces with error status",
        data: metrics.timeseries.map((b: MetricsBucket) => ({
          timestamp: b.timestamp,
          value: b.count > 0 ? (b.error_count / b.count) * 100 : 0
        })),
        color: "#ef4444",
        unit: "% errors",
        missingBuckets: false
      },
      {
        title: "Average Duration Over Time",
        description: "Average trace duration in milliseconds",
        data: metrics.timeseries.map((b: MetricsBucket) => ({
          timestamp: b.timestamp,
          value: b.avg_duration ?? null
        })),
        color: "#10b981",
        unit: "ms",
        missingBuckets: metrics.timeseries.some(
          (bucket) => bucket.avg_duration == null
        )
      }
    ];
  }, [metrics]);

  const hasGrafana = !!grafanaBaseUrl;
  const windowMeta = useMemo(() => {
    if (!metrics) return undefined;
    const windowMinutes = metrics.window_minutes;
    const startTime = new Date(metrics.start_time);
    const endTime = new Date(startTime.getTime() + windowMinutes * 60 * 1000);
    return `Last ${windowMinutes} minutes · ${startTime.toLocaleString()} → ${endTime.toLocaleString()}`;
  }, [metrics]);

  const sharedMeta = useMemo(() => {
    if (!metrics && !asOfLabel) return undefined;
    return {
      window: windowMeta,
      asOf: asOfLabel || undefined
    };
  }, [windowMeta, asOfLabel, metrics]);

  return (
    <>
      <header className="hero">
        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-end" }}>
          <div>
            <p className="kicker">Observability</p>
            <h1>Metrics (Preview)</h1>
            <p className="subtitle">
              Server-side aggregated metrics. For advanced dashboards, use Grafana.
            </p>
          </div>
          <div style={{ marginBottom: "8px" }}>
            <select
              value={window}
              onChange={(e) => setWindow(e.target.value)}
              style={{
                padding: "8px 12px",
                borderRadius: "8px",
                border: "1px solid var(--border)",
                backgroundColor: "var(--surface)",
                color: "var(--ink)",
                fontWeight: 500,
                cursor: "pointer"
              }}
            >
              {WINDOW_OPTIONS.map((opt) => (
                <option key={opt.value} value={opt.value}>
                  {opt.label}
                </option>
              ))}
            </select>
          </div>
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
              Live System Data
            </div>
            <div style={{ color: "var(--muted)", fontSize: "0.9rem" }}>
              These metrics are aggregated across the entire telemetry store for the selected window.
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

      <div
        style={{
          display: "grid",
          gridTemplateColumns: "repeat(auto-fit, minmax(400px, 1fr))",
          gap: "24px"
        }}
      >
        {panels.map((panel) => (
          <MetricCard
            key={panel.title}
            panel={panel}
            isLoading={isLoading}
            error={error}
            meta={sharedMeta}
          />
        ))}
      </div>

      {!isLoading && !error && metrics && metrics.summary.total_count === 0 && (
        <div className="panel" style={{ textAlign: "center", padding: "60px" }}>
          <div style={{ fontSize: "1.2rem", marginBottom: "12px" }}>
            No trace data available in this window
          </div>
          <div style={{ color: "var(--muted)", marginBottom: "20px" }}>
            Try selecting a larger window or generate some activity in the Agent Chat.
          </div>
          <Link
            to="/"
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
            Go to Agent Chat
          </Link>
        </div>
      )}

      {!isLoading && !error && metrics && metrics.summary.total_count > 0 && (
        <div style={{ marginTop: "32px", padding: "16px", backgroundColor: "var(--surface-muted)", borderRadius: "8px", border: "1px solid var(--border)" }}>
          <div style={{ fontWeight: 600, marginBottom: "8px" }}>Window Summary</div>
          <div style={{ fontSize: "0.9rem", color: "var(--ink)", display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(200px, 1fr))", gap: "16px" }}>
              <div>
                  <div style={{ color: "var(--muted)", marginBottom: "4px" }}>Total Traces</div>
                  <div style={{ fontSize: "1.2rem", fontWeight: 700 }}>{metrics.summary.total_count}</div>
              </div>
              <div>
                  <div style={{ color: "var(--muted)", marginBottom: "4px" }}>Errors</div>
                  <div style={{ fontSize: "1.2rem", fontWeight: 700, color: metrics.summary.error_count > 0 ? "var(--error)" : "inherit" }}>
                      {metrics.summary.error_count} ({((metrics.summary.error_count / metrics.summary.total_count) * 100).toFixed(1)}%)
                  </div>
              </div>
              <div>
                  <div style={{ color: "var(--muted)", marginBottom: "4px" }}>Avg Duration</div>
                  <div style={{ fontSize: "1.2rem", fontWeight: 700 }}>{metrics.summary.avg_duration?.toFixed(0)} ms</div>
              </div>
              <div>
                  <div style={{ color: "var(--muted)", marginBottom: "4px" }}>P95 Duration</div>
                  <div style={{ fontSize: "1.2rem", fontWeight: 700 }}>{metrics.summary.p95_duration?.toFixed(0)} ms</div>
              </div>
          </div>
          <div style={{ marginTop: "16px", paddingTop: "16px", borderTop: "1px solid var(--border)", fontSize: "0.85rem", color: "var(--muted)" }}>
              <strong>Window Start:</strong> {new Date(metrics.start_time).toLocaleString()}
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
