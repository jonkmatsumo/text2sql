import React from "react";
import { SpanDetail } from "../../types";

interface PromptViewerProps {
  span: SpanDetail | null;
}

function renderPayload(value: any) {
  if (value == null) return "—";
  if (typeof value === "string") return value;
  return JSON.stringify(value, null, 2);
}

export default function PromptViewer({ span }: PromptViewerProps) {
  if (!span) {
    return (
      <div className="trace-panel">
        <h3>Prompt Inspection</h3>
        <p className="subtitle">Select a span to inspect prompts and tool payloads.</p>
      </div>
    );
  }

  const payloadMap = new Map<string, any>();
  (span.payloads || []).forEach((payload) => {
    if (payload.payload_type) {
      payloadMap.set(payload.payload_type as string, payload.payload_json ?? payload.blob_url);
    }
  });

  const attr = span.span_attributes || {};
  const systemPrompt = payloadMap.get("llm.prompt.system") ?? attr["llm.prompt.system"];
  const userPrompt = payloadMap.get("llm.prompt.user") ?? attr["llm.prompt.user"];
  const responseText = payloadMap.get("llm.response.text") ?? attr["llm.response.text"];
  const toolInputs = payloadMap.get("telemetry.inputs_json") ?? attr["telemetry.inputs_json"];
  const toolOutputs = payloadMap.get("telemetry.outputs_json") ?? attr["telemetry.outputs_json"];
  const errorPayload = payloadMap.get("telemetry.error_json") ?? attr["telemetry.error_json"];

  return (
    <div className="trace-panel">
      <h3>Prompt Inspection</h3>
      <div className="trace-panel__section">
        <h4>System Prompt</h4>
        <pre>{renderPayload(systemPrompt)}</pre>
      </div>
      <div className="trace-panel__section">
        <h4>User Prompt</h4>
        <pre>{renderPayload(userPrompt)}</pre>
      </div>
      <div className="trace-panel__section">
        <h4>Model Response</h4>
        <pre>{renderPayload(responseText)}</pre>
      </div>
      <div className="trace-panel__section">
        <h4>Tool Inputs</h4>
        <pre>{renderPayload(toolInputs)}</pre>
      </div>
      <div className="trace-panel__section">
        <h4>Tool Outputs</h4>
        <pre>{renderPayload(toolOutputs)}</pre>
      </div>
      {errorPayload && (
        <div className="trace-panel__section">
          <h4>Errors</h4>
          <pre>{renderPayload(errorPayload)}</pre>
        </div>
      )}
    </div>
  );
}
