import React from "react";
import { WaterfallRow } from "./waterfall_model";

interface WaterfallSpanRowProps {
  row: WaterfallRow;
  traceStart: number;
  totalDuration: number;
  onSelect: (spanId: string) => void;
  isCriticalPath?: boolean;
}

function formatMs(value: number) {
  if (value < 1000) return `${value} ms`;
  return `${(value / 1000).toFixed(2)} s`;
}

export const WaterfallSpanRow: React.FC<WaterfallSpanRowProps> = ({
  row,
  traceStart,
  totalDuration,
  onSelect,
  isCriticalPath
}) => {
  const startMs = new Date(row.span.start_time).getTime();
  const offsetPct = ((startMs - traceStart) / totalDuration) * 100;
  const widthPct = (row.span.duration_ms / totalDuration) * 100;

  return (
    <button
      type="button"
      className={`trace-waterfall__row ${isCriticalPath ? "trace-waterfall__row--critical" : ""}`}
      onClick={() => onSelect(row.span.span_id)}
    >
      <div className="trace-waterfall__label" style={{ paddingLeft: row.depth * 14 }}>
        <span className={`status-pill status-pill--${row.span.status_code}`}>
          {row.span.status_code.replace("STATUS_CODE_", "")}
        </span>
        <span style={{ fontWeight: isCriticalPath ? 600 : 400 }}>{row.span.name}</span>
      </div>
      <div className="trace-waterfall__bar-wrap">
        <div
          className={`trace-waterfall__bar ${isCriticalPath ? "trace-waterfall__bar--critical" : ""}`}
          style={{
            marginLeft: `${Math.max(0, offsetPct)}%`,
            width: `${Math.max(2, widthPct)}%`
          }}
        />
        <span className="trace-waterfall__duration">
          {formatMs(row.span.duration_ms)}
        </span>
      </div>
    </button>
  );
};
