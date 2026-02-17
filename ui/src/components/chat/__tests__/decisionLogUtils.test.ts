import { describe, it, expect } from "vitest";
import type { NormalizedDecisionEvent } from "../../../utils/observability";
import {
  countWarningAndErrorEvents,
  filterDecisionEvents,
  getDecisionPhases,
  mapDecisionEventSeverity,
} from "../decisionLogUtils";

function ev(
  key: string,
  event: Record<string, unknown>,
  timestampMs: number | null = null
): NormalizedDecisionEvent {
  return {
    key,
    event,
    timestampMs,
  };
}

describe("decisionLogUtils", () => {
  it("maps decision severity consistently", () => {
    expect(mapDecisionEventSeverity({ level: "ERROR" })).toBe("error");
    expect(mapDecisionEventSeverity({ type: "warn" })).toBe("warn");
    expect(mapDecisionEventSeverity({ event_type: "debug" })).toBe("info");
    expect(mapDecisionEventSeverity({ type: "unknown" })).toBe("neutral");
    expect(mapDecisionEventSeverity(undefined)).toBe("neutral");
  });

  it("counts warning and error events only", () => {
    const events: NormalizedDecisionEvent[] = [
      ev("1", { type: "warn" }),
      ev("2", { level: "error" }),
      ev("3", { type: "info" }),
      ev("4", { decision: "none" }),
    ];
    expect(countWarningAndErrorEvents(events)).toBe(2);
  });

  it("returns sorted unique decision phases", () => {
    const phases = getDecisionPhases([
      ev("1", { node: "execute" }),
      ev("2", { phase: "router" }),
      ev("3", { node: "execute" }),
      ev("4", { phase: "plan" }),
    ]);
    expect(phases).toEqual(["execute", "plan", "router"]);
  });

  it("filters by search and phase while preserving original order", () => {
    const events: NormalizedDecisionEvent[] = [
      ev("1", { node: "router", decision: "route request", reason: "first" }),
      ev("2", { node: "execute", decision: "retry query", reason: "second", type: "warn" }),
      ev("3", { node: "execute", decision: "query failed", reason: "third", level: "error" }),
    ];

    const searchOnly = filterDecisionEvents(events, "query", "all");
    expect(searchOnly.map((item) => item.key)).toEqual(["2", "3"]);

    const phaseAndSearch = filterDecisionEvents(events, "query", "execute");
    expect(phaseAndSearch.map((item) => item.key)).toEqual(["2", "3"]);

    const phaseOnly = filterDecisionEvents(events, "", "router");
    expect(phaseOnly.map((item) => item.key)).toEqual(["1"]);
  });
});
