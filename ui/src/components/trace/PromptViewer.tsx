import React, { useState } from "react";
import { SpanDetail } from "../../types";
import { ArtifactPanel } from "../artifacts/ArtifactPanel";

interface PromptViewerProps {
  span: SpanDetail | null;
}

type TabType = "prompt" | "response" | "tool" | "metadata";

export default function PromptViewer({ span }: PromptViewerProps) {
  const [activeTab, setActiveTab] = useState<TabType>("prompt");

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

  const getArtifact = (type: string, attrKey: string, title: string) => {
    const content = payloadMap.get(type) ?? attr[attrKey];
    if (content == null && !["llm.prompt.user", "llm.response.text"].includes(type)) return null;

    const meta = payloadMetadataMap.get(type);
    return (
      <ArtifactPanel
        key={type}
        title={title}
        content={content}
        payloadType={meta?.payload_type}
        hash={meta?.payload_hash}
        size={meta?.payload_size}
        blobUrl={meta?.blob_url}
        isRedacted={meta?.is_redacted}
      />
    );
  };

  const renderTabContent = () => {
    switch (activeTab) {
      case "prompt":
        return (
          <>
            {getArtifact("llm.prompt.system", "llm.prompt.system", "System Prompt")}
            {getArtifact("llm.prompt.user", "llm.prompt.user", "User Prompt")}
          </>
        );
      case "response":
        return (
          <>
            {getArtifact("llm.response.text", "llm.response.text", "Model Response")}
            {getArtifact("telemetry.error_json", "telemetry.error_json", "Errors")}
          </>
        );
      case "tool":
        return (
          <>
            {getArtifact("telemetry.inputs_json", "telemetry.inputs_json", "Tool Inputs")}
            {getArtifact("telemetry.outputs_json", "telemetry.outputs_json", "Tool Outputs")}
          </>
        );
      case "metadata":
        return (
          <div className="artifact-panel">
            <div className="artifact-panel__header">
              <h4 className="artifact-panel__title">Span Attributes</h4>
            </div>
            <div className="artifact-panel__content">
              <pre className="artifact-panel__pre">
                <code>{JSON.stringify(attr, null, 2)}</code>
              </pre>
            </div>
          </div>
        );
      default:
        return null;
    }
  };

  return (
    <div className="trace-panel">
      <div className="trace-panel__header" style={{ marginBottom: "16px" }}>
        <h3>Prompt Inspection</h3>
      </div>

      <div className="segmented-control" style={{ marginBottom: "16px" }}>
        <button
          className={activeTab === "prompt" ? "active" : ""}
          onClick={() => setActiveTab("prompt")}
        >
          Prompt
        </button>
        <button
          className={activeTab === "response" ? "active" : ""}
          onClick={() => setActiveTab("response")}
        >
          Response
        </button>
        <button
          className={activeTab === "tool" ? "active" : ""}
          onClick={() => setActiveTab("tool")}
        >
          Tool I/O
        </button>
        <button
          className={activeTab === "metadata" ? "active" : ""}
          onClick={() => setActiveTab("metadata")}
        >
          Metadata
        </button>
      </div>

      <div className="prompt-viewer__content">
        {renderTabContent()}
      </div>
    </div>
  );
}
