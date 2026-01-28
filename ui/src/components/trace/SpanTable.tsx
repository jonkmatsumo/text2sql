import React from "react";
import { SpanSummary } from "../../types";
import VirtualList from "../common/VirtualList";

interface SpanTableProps {
  spans: SpanSummary[];
  onSelect: (spanId: string) => void;
}

function formatMs(value: number) {
  if (value < 1000) return `${value} ms`;
  return `${(value / 1000).toFixed(2)} s`;
}

export default function SpanTable({ spans, onSelect }: SpanTableProps) {
  const height = Math.min(420, Math.max(220, spans.length * 36));

  return (
    <div className="trace-span-table">
      <div className="trace-span-table__header">
        <span>Name</span>
        <span>Status</span>
        <span>Kind</span>
        <span>Duration</span>
      </div>
      <VirtualList
        items={spans}
        rowHeight={36}
        height={height}
        renderRow={(span) => (
          <button
            type="button"
            className="trace-span-table__row"
            onClick={() => onSelect(span.span_id)}
          >
            <span className="trace-span-table__name">{span.name}</span>
            <span className={`status-pill status-pill--${span.status_code}`}>
              {span.status_code.replace("STATUS_CODE_", "")}
            </span>
            <span className="trace-span-table__kind">{span.kind}</span>
            <span className="trace-span-table__duration">{formatMs(span.duration_ms)}</span>
          </button>
        )}
      />
    </div>
  );
}
