import type { ReactNode } from "react";
import type { DiagnosticsFilterMode } from "./DiagnosticsFilters";

interface DiagnosticsStatusStripProps {
  status: "healthy" | "degraded" | "unknown";
  anomalyCount: number;
  filterMode: DiagnosticsFilterMode;
  onFilterModeChange: (mode: DiagnosticsFilterMode) => void;
  children?: ReactNode;
}

export function DiagnosticsStatusStrip({
  status,
  anomalyCount,
  filterMode,
  onFilterModeChange,
  children,
}: DiagnosticsStatusStripProps) {
  const statusLabel =
    status === "healthy" ? "Healthy" : status === "degraded" ? "Degraded" : "Unknown";
  const statusColor =
    status === "healthy"
      ? "var(--success)"
      : status === "degraded"
        ? "#f59e0b"
        : "var(--muted)";

  return (
    <div
      data-testid="diagnostics-status-strip"
      className="panel"
      style={{
        padding: "14px 16px",
        display: "flex",
        alignItems: "center",
        justifyContent: "space-between",
        gap: "12px",
        flexWrap: "wrap",
      }}
    >
      <div style={{ display: "flex", alignItems: "center", gap: "8px", flexWrap: "wrap" }}>
        <span
          style={{
            width: "10px",
            height: "10px",
            borderRadius: "50%",
            background: statusColor,
          }}
        />
        <strong>System Status: {statusLabel}</strong>
        <span style={{ color: "var(--muted)", fontSize: "0.85rem" }}>
          {anomalyCount} {anomalyCount === 1 ? "anomaly" : "anomalies"} detected
        </span>
      </div>
      <div style={{ display: "flex", gap: "8px", alignItems: "center" }}>
        {children}
        <button
          type="button"
          data-testid="diagnostics-filter-anomalies"
          onClick={() => onFilterModeChange("anomalies")}
          className="button-primary"
          style={{
            opacity: filterMode === "anomalies" ? 1 : 0.75,
            padding: "6px 12px",
          }}
        >
          Show only anomalies
        </button>
        <button
          type="button"
          data-testid="diagnostics-filter-all"
          onClick={() => onFilterModeChange("all")}
          className="button-primary"
          style={{
            opacity: filterMode === "all" ? 1 : 0.75,
            padding: "6px 12px",
          }}
        >
          Show all
        </button>
      </div>
    </div>
  );
}
