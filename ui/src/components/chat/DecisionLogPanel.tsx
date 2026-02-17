import { useMemo, useState } from "react";
import { CopyButton } from "../artifacts/CopyButton";
import { formatTimestamp, normalizeDecisionEvents, toPrettyJson } from "../../utils/observability";
import {
  countWarningAndErrorEvents,
  filterDecisionEvents,
  getDecisionPhases,
  getDecisionSeverityStyle,
  mapDecisionEventSeverity,
} from "./decisionLogUtils";

const MAX_VISIBLE_EVENTS = 10;

export interface DecisionLogPanelProps {
  events?: any[];
}

export function DecisionLogPanel({ events }: DecisionLogPanelProps) {
  const [showDetails, setShowDetails] = useState(false);
  const [showAllEvents, setShowAllEvents] = useState(false);
  const [searchQuery, setSearchQuery] = useState("");
  const [phaseFilter, setPhaseFilter] = useState("all");

  const normalizedEvents = useMemo(
    () => (events && events.length > 0 ? normalizeDecisionEvents(events) : []),
    [events]
  );
  const availablePhases = useMemo(
    () => getDecisionPhases(normalizedEvents),
    [normalizedEvents]
  );
  const filteredEvents = useMemo(
    () => filterDecisionEvents(normalizedEvents, searchQuery, phaseFilter),
    [normalizedEvents, searchQuery, phaseFilter]
  );
  const visibleEvents = useMemo(
    () => (showAllEvents ? filteredEvents : filteredEvents.slice(0, MAX_VISIBLE_EVENTS)),
    [filteredEvents, showAllEvents]
  );
  const hasHiddenEvents = filteredEvents.length > MAX_VISIBLE_EVENTS;
  const serializedDecisionLog = useMemo(
    () => toPrettyJson(normalizedEvents.map((item) => item.event)),
    [normalizedEvents]
  );
  const warningCount = useMemo(
    () => countWarningAndErrorEvents(normalizedEvents),
    [normalizedEvents]
  );

  if (normalizedEvents.length === 0) return null;

  return (
    <div className="decision-log" style={{ marginTop: "16px", borderTop: "1px solid var(--border-muted)", paddingTop: "12px", width: "100%" }}>
      <div
        data-testid="decision-log-summary"
        style={{
          fontSize: "0.82rem",
          color: "var(--muted)",
          display: "flex",
          alignItems: "center",
          justifyContent: "space-between",
          gap: "8px",
          flexWrap: "wrap",
        }}
      >
        <span>
          Decision log: {normalizedEvents.length} events ({warningCount} warning{warningCount === 1 ? "" : "s"})
        </span>
        <button
          type="button"
          data-testid="decision-log-toggle"
          onClick={() => setShowDetails((prev) => !prev)}
          style={{
            border: "1px solid var(--border)",
            borderRadius: "8px",
            padding: "4px 10px",
            background: "var(--surface)",
            color: "var(--accent)",
            cursor: "pointer",
            fontSize: "0.78rem",
            fontWeight: 600,
          }}
        >
          {showDetails ? "Hide details" : "Show details"}
        </button>
      </div>
      {showDetails && (
        <div style={{ marginTop: "8px" }}>
          {normalizedEvents.length > 0 && (
            <div style={{ marginTop: "10px", display: "flex", justifyContent: "flex-end" }}>
              <CopyButton text={serializedDecisionLog} label="Copy decision log" />
            </div>
          )}
          <div style={{ marginTop: "10px", display: "flex", gap: "8px", alignItems: "center", flexWrap: "wrap" }}>
            <input
              type="text"
              value={searchQuery}
              data-testid="decision-log-search"
              aria-label="Search decision events"
              placeholder="Search decisions, node, type"
              onChange={(event) => setSearchQuery(event.target.value)}
              style={{
                minWidth: "220px",
                padding: "6px 10px",
                borderRadius: "8px",
                border: "1px solid var(--border)",
                fontSize: "0.8rem",
              }}
            />
            <select
              value={phaseFilter}
              data-testid="decision-log-phase-filter"
              aria-label="Filter decision events by phase"
              onChange={(event) => setPhaseFilter(event.target.value)}
              style={{
                padding: "6px 10px",
                borderRadius: "8px",
                border: "1px solid var(--border)",
                fontSize: "0.8rem",
              }}
            >
              <option value="all">All phases</option>
              {availablePhases.map((phase) => (
                <option key={phase} value={phase}>
                  {phase}
                </option>
              ))}
            </select>
            <span style={{ color: "var(--muted)", fontSize: "0.78rem" }}>
              {filteredEvents.length} match{filteredEvents.length === 1 ? "" : "es"}
            </span>
          </div>
          <div style={{ marginTop: "12px", display: "grid", gap: "10px" }}>
            {filteredEvents.length === 0 && (
              <div data-testid="decision-log-empty-filter" style={{ color: "var(--muted)", fontSize: "0.8rem" }}>
                No decision events match the current filters.
              </div>
            )}
            {visibleEvents.map((item) => {
              const ev = item.event;
              const timestampMs = item.timestampMs;
              const eventType = String(ev?.type ?? ev?.event_type ?? ev?.action ?? "").trim();
              const tone = mapDecisionEventSeverity(ev);
              const toneStyle = getDecisionSeverityStyle(tone);
              const payloadRaw = ev?.payload ?? ev?.details ?? ev?.metadata ?? ev?.context;
              const payloadText = payloadRaw == null
                ? ""
                : typeof payloadRaw === "string"
                  ? payloadRaw
                  : toPrettyJson(payloadRaw);
              const payloadLines = payloadText ? payloadText.split("\n").length : 0;
              const collapsePayloadByDefault = payloadText.length > 200 || payloadLines > 4;
              return (
                <div key={item.key} data-testid="decision-event-item" data-severity={tone} style={{
                  fontSize: "0.8rem",
                  padding: "10px 12px",
                  borderRadius: "8px",
                  background: toneStyle.background,
                  border: `1px solid ${toneStyle.borderColor}`,
                }}>
                  <div style={{ display: "flex", justifyContent: "space-between", marginBottom: "4px" }}>
                    <strong style={{ color: "var(--accent)" }}>{ev.node || "Agent"}</strong>
                    <span style={{ color: "var(--muted)", fontSize: "0.75rem" }}>
                      {formatTimestamp(timestampMs, { style: "time", fallback: "No timestamp" })}
                    </span>
                  </div>
                  {(eventType || tone !== "neutral") && (
                    <div style={{ marginBottom: "4px", color: "var(--muted)", fontSize: "0.74rem", textTransform: "uppercase", letterSpacing: "0.04em" }}>
                      {eventType || "event"} · <span data-testid="decision-event-severity" style={{ color: toneStyle.labelColor }}>{tone}</span>
                    </div>
                  )}
                  <div data-testid="decision-event-decision" style={{ fontWeight: 500, color: "var(--ink)" }}>{ev.decision}</div>
                  <div style={{ color: "var(--muted)", fontStyle: "italic", marginTop: "2px", lineHeight: "1.4" }}>{ev.reason}</div>
                  {ev.retry_count > 0 && (
                    <div style={{ marginTop: "6px", fontSize: "0.75rem", color: "#f59e0b", fontWeight: 600 }}>
                      Retry #{ev.retry_count} {ev.error_category ? `(${ev.error_category.replace(/_/g, " ")})` : ""}
                    </div>
                  )}
                  {payloadText && (
                    <details open={!collapsePayloadByDefault} style={{ marginTop: "8px" }}>
                      <summary style={{ cursor: "pointer", color: "var(--muted)", fontSize: "0.75rem" }}>
                        {collapsePayloadByDefault ? "Payload (collapsed)" : "Payload"}
                      </summary>
                      <pre data-testid="decision-event-payload" style={{ marginTop: "6px", fontSize: "0.72rem", overflowX: "auto" }}>
                        {payloadText}
                      </pre>
                    </details>
                  )}
                </div>
              );
            })}
            {hasHiddenEvents && (
              <button
                type="button"
                data-testid="decision-log-show-all"
                onClick={() => setShowAllEvents((prev) => !prev)}
                style={{
                  justifySelf: "start",
                  marginTop: "4px",
                  padding: "6px 10px",
                  borderRadius: "8px",
                  border: "1px solid var(--border)",
                  background: "var(--surface)",
                  color: "var(--accent)",
                  cursor: "pointer",
                  fontSize: "0.78rem",
                  fontWeight: 600,
                }}
              >
                {showAllEvents ? "Show first 10 events" : `Show all ${filteredEvents.length} events`}
              </button>
            )}
          </div>
        </div>
      )}
    </div>
  );
}
