import React from "react";
import { CopyButton } from "../artifacts/CopyButton";
import TraceLink from "./TraceLink";

export interface RunIdentifiersProps {
  traceId?: string;
  interactionId?: string;
  requestId?: string;
}

function normalizeId(value?: string): string | undefined {
  if (!value) return undefined;
  const trimmed = value.trim();
  return trimmed ? trimmed : undefined;
}

function shortId(value: string): string {
  return value.length > 8 ? `${value.slice(0, 8)}...` : value;
}

function RunIdentifiersComponent({
  traceId,
  interactionId,
  requestId,
}: RunIdentifiersProps) {
  const normalizedTraceId = normalizeId(traceId);
  const normalizedInteractionId = normalizeId(interactionId);
  const normalizedRequestId = normalizeId(requestId);
  const hasTraceLink = Boolean(normalizedTraceId || normalizedInteractionId);

  if (!hasTraceLink && !normalizedRequestId) return null;

  return (
    <div
      data-testid="run-identifiers"
      style={{
        display: "flex",
        gap: "8px",
        alignItems: "center",
        flexWrap: "wrap",
        fontSize: "0.78rem",
        color: "var(--muted)",
      }}
    >
      {hasTraceLink && (
        <div style={{ display: "inline-flex", alignItems: "center", gap: "6px", flexWrap: "wrap" }}>
          <TraceLink
            traceId={normalizedTraceId}
            interactionId={normalizedInteractionId}
            variant="button"
            showCopy={false}
          />
          {normalizedTraceId && (
            <>
              <span>trace: {shortId(normalizedTraceId)}</span>
              <CopyButton text={normalizedTraceId} label="Copy trace id" />
            </>
          )}
        </div>
      )}
      {normalizedRequestId && (
        <div style={{ display: "inline-flex", alignItems: "center", gap: "6px", flexWrap: "wrap" }}>
          <span>request: {shortId(normalizedRequestId)}</span>
          <CopyButton text={normalizedRequestId} label="Copy request id" />
        </div>
      )}
    </div>
  );
}

const RunIdentifiers = React.memo(RunIdentifiersComponent);
RunIdentifiers.displayName = "RunIdentifiers";

export default RunIdentifiers;
