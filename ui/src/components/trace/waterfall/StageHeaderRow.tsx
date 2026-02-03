import React from "react";

interface StageHeaderRowProps {
  label: string;
  spanCount: number;
  totalDurationMs: number;
  totalSelfTimeMs: number;
  isExpanded: boolean;
  onToggle: () => void;
}

function formatMs(value: number) {
  if (value < 1000) return `${value} ms`;
  return `${(value / 1000).toFixed(2)} s`;
}

export const StageHeaderRow: React.FC<StageHeaderRowProps> = ({
  label,
  spanCount,
  totalDurationMs,
  totalSelfTimeMs,
  isExpanded,
  onToggle
}) => {
  return (
    <div className="trace-waterfall__stage-header" onClick={onToggle}>
      <div className="trace-waterfall__stage-label">
        <span className={`trace-waterfall__stage-toggle ${isExpanded ? "is-expanded" : ""}`}>
          ▶
        </span>
        <span className="trace-waterfall__stage-title">{label}</span>
        <span className="trace-waterfall__stage-count">({spanCount} spans)</span>
      </div>
      <div className="trace-waterfall__stage-duration">
        {formatMs(totalDurationMs)} · self {formatMs(totalSelfTimeMs)}
      </div>
    </div>
  );
};
