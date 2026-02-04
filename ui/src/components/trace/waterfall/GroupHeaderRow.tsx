import React from "react";

interface GroupHeaderRowProps {
  label: string;
  spanCount: number;
  totalDurationMs: number;
  totalSelfTimeMs?: number;
  isExpanded: boolean;
  onToggle: () => void;
  criticalPathSpanCount?: number;
}

function formatMs(value: number) {
  if (value < 1000) return `${value} ms`;
  return `${(value / 1000).toFixed(2)} s`;
}

export const GroupHeaderRow: React.FC<GroupHeaderRowProps> = ({
  label,
  spanCount,
  totalDurationMs,
  totalSelfTimeMs,
  isExpanded,
  onToggle,
  criticalPathSpanCount
}) => {
  return (
    <div className="trace-waterfall__group-header" onClick={onToggle}>
      <div className="trace-waterfall__group-label">
        <span className={`trace-waterfall__group-toggle ${isExpanded ? "is-expanded" : ""}`}>
          ▶
        </span>
        <span className="trace-waterfall__group-title">{label}</span>
        <span className="trace-waterfall__group-count">({spanCount} spans)</span>
        {criticalPathSpanCount && criticalPathSpanCount > 0 && (
          <span className="trace-waterfall__group-critical-badge">
            {criticalPathSpanCount} on critical path
          </span>
        )}
      </div>
      <div className="trace-waterfall__group-duration">
        {formatMs(totalDurationMs)} (agg)
        {totalSelfTimeMs != null ? ` · self ${formatMs(totalSelfTimeMs)}` : ""}
      </div>
    </div>
  );
};
