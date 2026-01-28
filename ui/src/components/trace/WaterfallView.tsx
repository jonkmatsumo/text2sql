import React, { useMemo } from "react";
import VirtualList from "../common/VirtualList";
import { SpanSummary } from "../../types";

export interface WaterfallRow {
  span: SpanSummary;
  depth: number;
}

interface WaterfallViewProps {
  rows: WaterfallRow[];
  traceStart: number;
  traceDurationMs: number;
  onSelect: (spanId: string) => void;
}

function formatMs(value: number) {
  if (value < 1000) return `${value} ms`;
  return `${(value / 1000).toFixed(2)} s`;
}

export default function WaterfallView({
  rows,
  traceStart,
  traceDurationMs,
  onSelect
}: WaterfallViewProps) {
  const height = Math.min(520, Math.max(240, rows.length * 28));
  const totalDuration = traceDurationMs || 1;

  const renderedRows = useMemo(() => rows, [rows]);

  return (
    <div className="trace-waterfall">
      <div className="trace-waterfall__header">
        <span>Span</span>
        <span>Duration</span>
      </div>
      <VirtualList
        items={renderedRows}
        rowHeight={28}
        height={height}
        renderRow={(row) => {
          const startMs = new Date(row.span.start_time).getTime();
          const offsetPct = ((startMs - traceStart) / totalDuration) * 100;
          const widthPct = (row.span.duration_ms / totalDuration) * 100;
          return (
            <button
              type="button"
              className="trace-waterfall__row"
              onClick={() => onSelect(row.span.span_id)}
            >
              <div className="trace-waterfall__label" style={{ paddingLeft: row.depth * 14 }}>
                <span className={`status-pill status-pill--${row.span.status_code}`}>
                  {row.span.status_code.replace("STATUS_CODE_", "")}
                </span>
                <span>{row.span.name}</span>
              </div>
              <div className="trace-waterfall__bar-wrap">
                <div
                  className="trace-waterfall__bar"
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
        }}
      />
    </div>
  );
}
