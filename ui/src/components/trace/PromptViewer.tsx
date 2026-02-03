import React from "react";
import { SpanDetail } from "../../types";
import { ArtifactPanel } from "../artifacts/ArtifactPanel";

interface PromptViewerProps {
  span: SpanDetail | null;
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
  const payloadMetadataMap = new Map<string, any>();

  (span.payloads || []).forEach((payload) => {
    if (payload.payload_type) {
      const type = payload.payload_type as string;
      payloadMap.set(type, payload.payload_json ?? payload.payload_text);
      payloadMetadataMap.set(type, payload);
    }
  });

  const attr = span.span_attributes || {};

  const sections = [
    { title: "System Prompt", type: "llm.prompt.system", attrKey: "llm.prompt.system" },
    { title: "User Prompt", type: "llm.prompt.user", attrKey: "llm.prompt.user" },
    { title: "Model Response", type: "llm.response.text", attrKey: "llm.response.text" },
    { title: "Tool Inputs", type: "telemetry.inputs_json", attrKey: "telemetry.inputs_json" },
    { title: "Tool Outputs", type: "telemetry.outputs_json", attrKey: "telemetry.outputs_json" },
    { title: "Errors", type: "telemetry.error_json", attrKey: "telemetry.error_json" },
  ];

  return (
    <div className="trace-panel">
      <h3>Prompt Inspection</h3>
      <div className="prompt-viewer__content" style={{ marginTop: "16px" }}>
        {sections.map((section) => {
          const content = payloadMap.get(section.type) ?? attr[section.attrKey];
          if (content == null && section.type !== "telemetry.error_json") {
             // We can decide to hide empty sections or show them as empty
             // For now, only show if there's content or it's a primary section
          }

          if (content == null && !["llm.prompt.user", "llm.response.text"].includes(section.type)) {
              return null;
          }

          const meta = payloadMetadataMap.get(section.type);

          return (
            <ArtifactPanel
              key={section.type}
              title={section.title}
              content={content}
              payloadType={meta?.payload_type}
              hash={meta?.payload_hash}
              size={meta?.payload_size}
              blobUrl={meta?.blob_url}
              isRedacted={meta?.is_redacted}
            />
          );
        })}
      </div>
    </div>
  );
}
