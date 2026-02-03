import React from "react";
import { SpanDetail } from "../../types";
import { ArtifactPanel } from "../artifacts/ArtifactPanel";
import { SpanEventList } from "./SpanEventList";
import { SpanLinksList } from "./SpanLinksList";

interface SpanDetailDrawerProps {
  span: SpanDetail | null;
  onClose: () => void;
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
        <h4 style={{ marginBottom: "8px" }}>Status</h4>
        <span className={`status-pill status-pill--${span.status_code}`}>
          {span.status_code.replace("STATUS_CODE_", "")}
        </span>
      </div>

      <div className="trace-drawer__section">
        <h4 style={{ marginBottom: "8px" }}>Timing</h4>
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
        <h4 style={{ marginBottom: "8px" }}>Token Usage</h4>
        <div className="trace-drawer__grid">
          <div>
            <span>Prompt</span>
            <strong>{tokenInputs != null ? String(tokenInputs) : "—"}</strong>
          </div>
          <div>
            <span>Completion</span>
            <strong>{tokenOutputs != null ? String(tokenOutputs) : "—"}</strong>
          </div>
          <div>
            <span>Total</span>
            <strong>{tokenTotal != null ? String(tokenTotal) : "—"}</strong>
          </div>
        </div>
      </div>

      <div className="trace-drawer__section">
        <ArtifactPanel
          title="Attributes"
          content={attrs}
          payloadType="span.attributes"
        />
      </div>

      {span.events && span.events.length > 0 && (
        <div className="trace-drawer__section">
          <h4 style={{ marginBottom: "8px" }}>Events</h4>
          <SpanEventList events={span.events} spanStartTime={span.start_time} />
        </div>
      )}

      {span.links && span.links.length > 0 && (
        <div className="trace-drawer__section">
          <h4 style={{ marginBottom: "8px" }}>Links</h4>
          <SpanLinksList links={span.links} />
        </div>
      )}
    </aside>
  );
}
