import React from "react";
import { SpanDetail } from "../../types";

interface SpanDetailDrawerProps {
  span: SpanDetail | null;
  onClose: () => void;
}

function renderJson(value: any) {
  if (value == null) return "—";
  if (typeof value === "string") return value;
  return JSON.stringify(value, null, 2);
}

export default function SpanDetailDrawer({ span, onClose }: SpanDetailDrawerProps) {
  if (!span) return null;

  const attrs = span.span_attributes || {};
  const tokenInputs = attrs["llm.token_usage.input_tokens"];
  const tokenOutputs = attrs["llm.token_usage.output_tokens"];
  const tokenTotal = attrs["llm.token_usage.total_tokens"];

  return (
    <aside className="trace-drawer">
      <div className="trace-drawer__header">
        <div>
          <h3>{span.name}</h3>
          <p className="subtitle">Span {span.span_id.slice(0, 8)}...</p>
        </div>
        <button type="button" onClick={onClose} className="trace-drawer__close">
          Close
        </button>
      </div>

      <div className="trace-drawer__section">
        <h4>Status</h4>
        <span className={`status-pill status-pill--${span.status_code}`}>
          {span.status_code.replace("STATUS_CODE_", "")}
        </span>
      </div>

      <div className="trace-drawer__section">
        <h4>Timing</h4>
        <div className="trace-drawer__grid">
          <div>
            <span>Start</span>
            <strong>{new Date(span.start_time).toLocaleString()}</strong>
          </div>
          <div>
            <span>Duration</span>
            <strong>{span.duration_ms} ms</strong>
          </div>
        </div>
      </div>

      <div className="trace-drawer__section">
        <h4>Token Usage</h4>
        <div className="trace-drawer__grid">
          <div>
            <span>Prompt</span>
            <strong>{tokenInputs ?? "—"}</strong>
          </div>
          <div>
            <span>Completion</span>
            <strong>{tokenOutputs ?? "—"}</strong>
          </div>
          <div>
            <span>Total</span>
            <strong>{tokenTotal ?? "—"}</strong>
          </div>
        </div>
      </div>

      <div className="trace-drawer__section">
        <h4>Attributes</h4>
        <pre>{renderJson(attrs)}</pre>
      </div>

      <div className="trace-drawer__section">
        <h4>Events</h4>
        <pre>{renderJson(span.events)}</pre>
      </div>

      <div className="trace-drawer__section">
        <h4>Links</h4>
        <pre>{renderJson(span.links)}</pre>
      </div>
    </aside>
  );
}
