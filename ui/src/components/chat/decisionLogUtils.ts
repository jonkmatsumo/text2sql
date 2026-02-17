import type { NormalizedDecisionEvent } from "../../utils/observability";

export type DecisionSeverityTone = "neutral" | "info" | "warn" | "error";

export interface DecisionSeverityStyle {
  background: string;
  borderColor: string;
  labelColor: string;
}

export function mapDecisionEventSeverity(event: any): DecisionSeverityTone {
  const raw = String(
    event?.severity ?? event?.level ?? event?.type ?? event?.event_type ?? ""
  )
    .trim()
    .toLowerCase();
  if (!raw) return "neutral";
  if (raw.includes("error") || raw.includes("fail")) return "error";
  if (raw.includes("warn") || raw.includes("retry") || raw.includes("timeout")) return "warn";
  if (raw.includes("info") || raw.includes("debug")) return "info";
  return "neutral";
}

export function getDecisionSeverityStyle(tone: DecisionSeverityTone): DecisionSeverityStyle {
  if (tone === "error") {
    return {
      background: "rgba(220, 53, 69, 0.08)",
      borderColor: "rgba(220, 53, 69, 0.28)",
      labelColor: "var(--error)",
    };
  }
  if (tone === "warn") {
    return {
      background: "rgba(245, 158, 11, 0.08)",
      borderColor: "rgba(245, 158, 11, 0.3)",
      labelColor: "#b45309",
    };
  }
  if (tone === "info") {
    return {
      background: "rgba(59, 130, 246, 0.07)",
      borderColor: "rgba(59, 130, 246, 0.26)",
      labelColor: "#1d4ed8",
    };
  }
  return {
    background: "var(--surface-muted)",
    borderColor: "var(--border-muted)",
    labelColor: "var(--muted)",
  };
}

export function countWarningAndErrorEvents(events: NormalizedDecisionEvent[]): number {
  return events.filter((item) => {
    const tone = mapDecisionEventSeverity(item.event);
    return tone === "warn" || tone === "error";
  }).length;
}

export function getDecisionPhases(events: NormalizedDecisionEvent[]): string[] {
  return Array.from(
    new Set(
      events
        .map((item) => String(item.event?.node ?? item.event?.phase ?? "").trim())
        .filter(Boolean)
    )
  ).sort();
}

export function filterDecisionEvents(
  events: NormalizedDecisionEvent[],
  searchQuery: string,
  phaseFilter: string
): NormalizedDecisionEvent[] {
  const normalizedSearch = searchQuery.trim().toLowerCase();
  const normalizedPhase = phaseFilter.trim().toLowerCase();

  return events.filter((item) => {
    const event = item.event ?? {};
    const eventPhase = String(event.node ?? event.phase ?? "").trim().toLowerCase();
    if (normalizedPhase !== "all" && normalizedPhase && eventPhase !== normalizedPhase) {
      return false;
    }
    if (!normalizedSearch) return true;

    const haystack = [
      event.decision,
      event.reason,
      event.message,
      event.node,
      event.phase,
      event.type,
      event.event_type,
      event.action,
    ]
      .filter((value) => value != null)
      .map((value) => String(value).toLowerCase())
      .join(" ");
    return haystack.includes(normalizedSearch);
  });
}
