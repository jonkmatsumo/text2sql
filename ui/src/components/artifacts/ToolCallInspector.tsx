import React, { useMemo, useState } from "react";
import { CopyButton } from "./CopyButton";
import { SpanDetail } from "../../types";

type TabKey = "inputs" | "outputs" | "error" | "raw";

interface ToolCallInspectorProps {
  span: SpanDetail;
  inputs: any;
  outputs: any;
  error: any;
  onReveal?: () => void;
}

function detectToolName(span: SpanDetail) {
  const attrs = span.span_attributes || {};
  return (
    attrs["tool.name"] ||
    attrs["tool_name"] ||
    attrs["telemetry.tool_name"] ||
    attrs["llm.tool.name"] ||
    span.name
  );
}

function formatPayload(payload: any) {
  if (payload == null) return "—";
  if (typeof payload === "string") return payload;
  try {
    return JSON.stringify(payload, null, 2);
  } catch {
    return String(payload);
  }
}

export const ToolCallInspector: React.FC<ToolCallInspectorProps> = ({
  span,
  inputs,
  outputs,
  error,
  onReveal
}) => {
  const [activeTab, setActiveTab] = useState<TabKey>("inputs");
  const toolName = detectToolName(span);
  const status = span.status_code.replace("STATUS_CODE_", "");
  const latency = span.duration_ms;

  const rawPayload = useMemo(
    () => ({
      inputs,
      outputs,
      error
    }),
    [inputs, outputs, error]
  );

  const renderPayload = (payload: any) => (
    <div className="tool-inspector__payload">
      <div className="tool-inspector__payload-header">
        <CopyButton text={formatPayload(payload)} />
      </div>
      <pre>
        <code>{formatPayload(payload)}</code>
      </pre>
    </div>
  );

  return (
    <div className="tool-inspector">
      <div className="tool-inspector__header">
        <div>
          <h4>{toolName}</h4>
          <div className="tool-inspector__meta">
            <span>Status: {status}</span>
            <span>Latency: {latency} ms</span>
          </div>
        </div>
        {onReveal && (
          <button type="button" onClick={onReveal}>
            Reveal in waterfall
          </button>
        )}
      </div>

      <div className="segmented-control tool-inspector__tabs">
        <button
          className={activeTab === "inputs" ? "active" : ""}
          onClick={() => setActiveTab("inputs")}
        >
          Inputs
        </button>
        <button
          className={activeTab === "outputs" ? "active" : ""}
          onClick={() => setActiveTab("outputs")}
        >
          Outputs
        </button>
        <button
          className={activeTab === "error" ? "active" : ""}
          onClick={() => setActiveTab("error")}
        >
          Error
        </button>
        <button
          className={activeTab === "raw" ? "active" : ""}
          onClick={() => setActiveTab("raw")}
        >
          Raw
        </button>
      </div>

      {activeTab === "inputs" && renderPayload(inputs)}
      {activeTab === "outputs" && renderPayload(outputs)}
      {activeTab === "error" && renderPayload(error)}
      {activeTab === "raw" && renderPayload(rawPayload)}
    </div>
  );
};
