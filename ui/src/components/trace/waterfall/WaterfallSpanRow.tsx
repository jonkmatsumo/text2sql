import React, { useMemo } from "react";
import { WaterfallRow, extractSpanEventMarkers } from "./waterfall_model";

interface WaterfallSpanRowProps {
  row: WaterfallRow;
  traceStart: number;
  totalDuration: number;
  onSelect: (spanId: string) => void;
  isCriticalPath?: boolean;
  isSelected?: boolean;
  showEvents?: boolean;
  isMatch?: boolean;
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
  isCriticalPath,
  isSelected,
  showEvents = true,
  isMatch = false
}) => {
  const startMs = new Date(row.span.start_time).getTime();
  const offsetPct = ((startMs - traceStart) / totalDuration) * 100;
  const widthPct = (row.span.duration_ms / totalDuration) * 100;

  const markers = useMemo(() => {
    if (!showEvents) return [];
    return extractSpanEventMarkers(row.span, traceStart);
  }, [row.span, traceStart, showEvents]);

  const maxVisibleMarkers = 10;
  const visibleMarkers = markers.slice(0, maxVisibleMarkers);
  const overflowCount = markers.length - maxVisibleMarkers;
  const selfTimeMs = Number((row.span as any).self_time_ms ?? NaN);

  return (
    <button
      type="button"
      className={`trace-waterfall__row${isCriticalPath ? " trace-waterfall__row--critical" : ""}${isSelected ? " trace-waterfall__row--selected" : ""}${isMatch ? " trace-waterfall__row--match" : ""}`}
      onClick={() => onSelect(row.span.span_id)}
    >
      <div className="trace-waterfall__label" style={{ paddingLeft: row.depth * 14 }}>
        <span className={`status-pill status-pill--${row.span.status_code}`}>
          {row.span.status_code.replace("STATUS_CODE_", "")}
        </span>
        {isMatch && <span className="trace-waterfall__match-pill">Match</span>}
        <span style={{ fontWeight: isCriticalPath ? 600 : 400 }}>{row.span.name}</span>
      </div>
      <div className="trace-waterfall__bar-wrap">
        <div
          className={`trace-waterfall__bar ${isCriticalPath ? "trace-waterfall__bar--critical" : ""}`}
          style={{
            marginLeft: `${Math.max(0, offsetPct)}%`,
            width: `${Math.max(0.5, widthPct)}%`
          }}
        >
          {visibleMarkers.map((marker, i) => {
            const relativeOffsetPct = ((marker.ts - (startMs - traceStart)) / Math.max(1, row.span.duration_ms)) * 100;
            return (
              <div
                key={i}
                className="trace-waterfall__event-marker"
                style={{ left: `${Math.max(0, Math.min(100, relativeOffsetPct))}%` }}
                title={`${marker.name} (+${Math.round(marker.ts - (startMs - traceStart))}ms)`}
              />
            );
          })}
          {overflowCount > 0 && (
            <div
              className="trace-waterfall__event-overflow"
              style={{ left: "calc(100% + 4px)" }}
              title={`${overflowCount} more events`}
            >
              +{overflowCount}
            </div>
          )}
        </div>
        <span className="trace-waterfall__duration">
          {formatMs(row.span.duration_ms)}
          {Number.isFinite(selfTimeMs) ? ` · self ${formatMs(selfTimeMs)}` : ""}
        </span>
      </div>
    </button>
  );
};
