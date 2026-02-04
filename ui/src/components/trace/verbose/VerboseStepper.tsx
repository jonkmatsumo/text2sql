import React from "react";
import { VerboseArtifactPanel } from "./VerboseArtifactPanel";
import { VerboseStep } from "./verbose_step_builder";

interface VerboseStepperProps {
  steps: VerboseStep[];
  onRevealSpan?: (spanId: string) => void;
}

function formatDuration(durationMs: number) {
  if (durationMs < 1000) return `${durationMs.toFixed(0)} ms`;
  return `${(durationMs / 1000).toFixed(2)} s`;
}

function formatOffset(offsetMs: number) {
  if (offsetMs < 1000) return `${offsetMs.toFixed(0)} ms`;
  return `${(offsetMs / 1000).toFixed(2)} s`;
}

export function VerboseStepper({ steps, onRevealSpan }: VerboseStepperProps) {
  if (!steps.length) {
    return (
      <div className="trace-panel">
        <div className="trace-panel__header">
          <h3>Verbose / Diagnostic View</h3>
        </div>
        <div style={{ color: "var(--muted)" }}>
          No intermediate artifacts were captured for this trace.
        </div>
      </div>
    );
  }

  return (
    <div className="trace-panel">
      <div className="trace-panel__header">
        <h3>Verbose / Diagnostic View</h3>
      </div>
      <div style={{ display: "flex", flexDirection: "column", gap: "12px" }}>
        {steps.map((step) => (
          <details
            key={step.id}
            style={{
              border: "1px solid var(--border)",
              borderRadius: "10px",
              padding: "12px 14px",
              background: "var(--surface)"
            }}
          >
            <summary
              style={{
                display: "flex",
                alignItems: "center",
                justifyContent: "space-between",
                cursor: "pointer",
                gap: "12px"
              }}
            >
              <div style={{ display: "flex", alignItems: "center", gap: "10px" }}>
                <span style={{ fontWeight: 600 }}>{step.title}</span>
                <span
                  className={`artifact-badge artifact-badge--${
                    step.status === "error" ? "danger" : "info"
                  }`}
                >
                  {step.status === "error" ? "Error" : "OK"}
                </span>
              </div>
              <div style={{ fontSize: "0.85rem", color: "var(--muted)" }}>
                {formatDuration(step.durationMs)} · +{formatOffset(step.startOffsetMs)}
              </div>
            </summary>

            <div style={{ marginTop: "12px" }}>
              <div style={{ display: "flex", gap: "8px", marginBottom: "12px" }}>
                <button
                  type="button"
                  className="trace-link__btn"
                  onClick={() => onRevealSpan?.(step.spanId)}
                >
                  Reveal in Waterfall
                </button>
                <button
                  type="button"
                  className="trace-link__btn"
                  onClick={() => onRevealSpan?.(step.spanId)}
                >
                  Open Span
                </button>
              </div>

              {step.artifacts.length === 0 ? (
                <div style={{ color: "var(--muted)" }}>
                  No structured artifacts captured for this step.
                </div>
              ) : (
                step.artifacts.map((artifact) => (
                  <VerboseArtifactPanel key={artifact.id} artifact={artifact} />
                ))
              )}
            </div>
          </details>
        ))}
      </div>
    </div>
  );
}
