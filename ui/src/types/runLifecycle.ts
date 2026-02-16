/**
 * Canonical run lifecycle states for AgentChat.
 * Replaces scattered boolean flags (isLoading, currentPhase, error, etc.)
 * with a single discriminated state machine.
 */
export type RunStatus =
  | "idle"
  | "streaming"
  | "finalizing"
  | "succeeded"
  | "failed"
  | "canceled";

/** Canonical phase names emitted by the SSE stream. */
export const PHASE_ORDER = [
  "router",
  "plan",
  "execute",
  "synthesize",
  "visualize",
] as const;

export type Phase = (typeof PHASE_ORDER)[number];

/** Returns true if `phase` is a recognized canonical phase. */
export function isCanonicalPhase(phase: string): phase is Phase {
  return (PHASE_ORDER as readonly string[]).includes(phase);
}

/**
 * Returns the index of a phase in the canonical order, or -1 if unknown.
 * Used for monotonic-advance checks.
 */
export function phaseIndex(phase: string): number {
  return (PHASE_ORDER as readonly string[]).indexOf(phase as Phase);
}
