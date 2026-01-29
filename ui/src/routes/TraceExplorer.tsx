import React, { useState } from "react";
import { useNavigate } from "react-router-dom";
import { grafanaBaseUrl, buildGrafanaTraceUrl, isGrafanaConfigured } from "../config";

const TRACE_ID_RE = /^[0-9a-f]{32}$/i;
const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

export default function TraceExplorer() {
  const navigate = useNavigate();
  const [traceId, setTraceId] = useState("");
  const [interactionId, setInteractionId] = useState("");
  const [error, setError] = useState<string | null>(null);

  const isValidTraceId = traceId.trim() ? TRACE_ID_RE.test(traceId.trim()) : false;
  const isValidInteractionId = interactionId.trim() ? UUID_RE.test(interactionId.trim()) : false;
  const canOpen = isValidTraceId || isValidInteractionId;

  const handleOpenTrace = () => {
    setError(null);

    if (isValidTraceId) {
      navigate(`/traces/${traceId.trim()}`);
      return;
    }

    if (isValidInteractionId) {
      navigate(`/traces/interaction/${interactionId.trim()}`);
      return;
    }

    setError("Please enter a valid trace ID (32 hex chars) or interaction ID (UUID).");
  };

  const grafanaTraceUrl = isValidTraceId ? buildGrafanaTraceUrl(traceId.trim()) : undefined;

  return (
    <>
      <header className="hero">
        <div>
          <p className="kicker">Observability</p>
          <h1>Trace Explorer</h1>
          <p className="subtitle">
            View detailed trace information including span waterfall and execution logs.
          </p>
        </div>
      </header>

      <div style={{ display: "grid", gap: "24px" }}>
        <div className="panel">
          <h3>Open by ID</h3>
          <p className="subtitle">
            Enter a trace ID or interaction ID to view the trace detail.
          </p>

          <div style={{ display: "grid", gap: "16px", marginTop: "16px" }}>
            <div>
              <label
                htmlFor="trace-id-input"
                style={{ display: "block", marginBottom: "6px", fontWeight: 500 }}
              >
                Trace ID
              </label>
              <input
                id="trace-id-input"
                type="text"
                placeholder="32-character hex string (e.g., abc123...)"
                value={traceId}
                onChange={(e) => {
                  setTraceId(e.target.value);
                  setError(null);
                }}
                style={{
                  width: "100%",
                  padding: "10px 12px",
                  borderRadius: "8px",
                  border: "1px solid var(--border)",
                  fontFamily: "monospace",
                  fontSize: "0.9rem"
                }}
              />
              {traceId.trim() && !isValidTraceId && (
                <p style={{ color: "var(--error)", fontSize: "0.85rem", marginTop: "4px" }}>
                  Invalid format. Expected 32 hex characters.
                </p>
              )}
            </div>

            <div style={{ textAlign: "center", color: "var(--muted)", fontWeight: 500 }}>
              — or —
            </div>

            <div>
              <label
                htmlFor="interaction-id-input"
                style={{ display: "block", marginBottom: "6px", fontWeight: 500 }}
              >
                Interaction ID
              </label>
              <input
                id="interaction-id-input"
                type="text"
                placeholder="UUID format (e.g., 123e4567-e89b-...)"
                value={interactionId}
                onChange={(e) => {
                  setInteractionId(e.target.value);
                  setError(null);
                }}
                style={{
                  width: "100%",
                  padding: "10px 12px",
                  borderRadius: "8px",
                  border: "1px solid var(--border)",
                  fontFamily: "monospace",
                  fontSize: "0.9rem"
                }}
              />
              {interactionId.trim() && !isValidInteractionId && (
                <p style={{ color: "var(--error)", fontSize: "0.85rem", marginTop: "4px" }}>
                  Invalid format. Expected UUID.
                </p>
              )}
            </div>
          </div>

          {error && (
            <p style={{ color: "var(--error)", marginTop: "12px" }}>{error}</p>
          )}

          <div style={{ display: "flex", gap: "12px", marginTop: "20px" }}>
            <button
              onClick={handleOpenTrace}
              disabled={!canOpen}
              style={{
                flex: 1,
                padding: "12px 24px",
                borderRadius: "10px",
                border: "none",
                backgroundColor: canOpen ? "var(--accent)" : "var(--surface-muted)",
                color: canOpen ? "#fff" : "var(--muted)",
                fontWeight: 600,
                cursor: canOpen ? "pointer" : "not-allowed"
              }}
            >
              Open Trace
            </button>

            {isGrafanaConfigured() && grafanaTraceUrl && (
              <a
                href={grafanaTraceUrl}
                target="_blank"
                rel="noreferrer"
                style={{
                  padding: "12px 24px",
                  borderRadius: "10px",
                  border: "1px solid var(--border)",
                  backgroundColor: "transparent",
                  color: "var(--ink)",
                  fontWeight: 500,
                  textDecoration: "none",
                  display: "flex",
                  alignItems: "center",
                  gap: "6px"
                }}
              >
                Open in Grafana
                <svg
                  width="14"
                  height="14"
                  viewBox="0 0 24 24"
                  fill="none"
                  stroke="currentColor"
                  strokeWidth="2"
                  strokeLinecap="round"
                  strokeLinejoin="round"
                >
                  <path d="M18 13v6a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h6" />
                  <polyline points="15 3 21 3 21 9" />
                  <line x1="10" y1="14" x2="21" y2="3" />
                </svg>
              </a>
            )}
          </div>
        </div>

        <div className="panel">
          <h3>Search Traces</h3>
          <p className="subtitle">
            Browse and filter traces from the telemetry store.
          </p>
          <button
            onClick={() => navigate("/admin/traces/search")}
            style={{
              marginTop: "12px",
              padding: "12px 24px",
              borderRadius: "10px",
              border: "1px solid var(--border)",
              backgroundColor: "transparent",
              color: "var(--ink)",
              fontWeight: 500,
              cursor: "pointer"
            }}
          >
            Open Trace Search
          </button>
        </div>
      </div>
    </>
  );
}
