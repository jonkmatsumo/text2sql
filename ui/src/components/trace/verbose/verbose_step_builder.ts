import { SpanSummary } from "../../../types";

export type VerboseStatus = "ok" | "error";

export interface VerboseArtifact {
  id: string;
  title: string;
  content: unknown;
  payloadType?: string;
  size?: number;
  hash?: string;
  isRedacted?: boolean;
  isTruncated?: boolean;
}

export interface VerboseStep {
  id: string;
  title: string;
  status: VerboseStatus;
  durationMs: number;
  startOffsetMs: number;
  spanId: string;
  traceId: string;
  artifacts: VerboseArtifact[];
}

const TITLE_MAP: Record<string, string> = {
  retrieve_context: "Retrieve Schema Context",
  plan_sql: "Plan SQL",
  generate_sql: "Generate SQL",
  validate_sql: "Validate SQL",
  execute_sql: "Execute SQL",
  cache_lookup: "Cache Lookup",
  visualize_query: "Build Visualization",
  clarify_query: "Clarify Question",
  correct_sql: "Correct SQL"
};

const ARTIFACT_KEYS: Array<{ key: string; title: string }> = [
  { key: "telemetry.inputs_json", title: "Inputs" },
  { key: "telemetry.outputs_json", title: "Outputs" },
  { key: "telemetry.error_json", title: "Errors" },
  { key: "llm.prompt.system", title: "System Prompt" },
  { key: "llm.prompt.user", title: "User Prompt" },
  { key: "llm.response.text", title: "Model Response" }
];

function safeParsePayload(value: unknown) {
  if (typeof value !== "string") return value;
  const trimmed = value.trim();
  if (!trimmed) return value;
  if (!(trimmed.startsWith("{") || trimmed.startsWith("["))) return value;
  try {
    return JSON.parse(trimmed);
  } catch {
    return value;
  }
}

function containsRedaction(value: unknown): boolean {
  if (value == null) return false;
  if (typeof value === "string") return value.includes("[REDACTED]");
  if (Array.isArray(value)) return value.some(containsRedaction);
  if (typeof value === "object") {
    return Object.values(value as Record<string, unknown>).some(containsRedaction);
  }
  return false;
}

function getNumber(value: unknown): number | undefined {
  if (typeof value === "number") return value;
  if (typeof value === "string" && value.trim() !== "") {
    const parsed = Number(value);
    return Number.isNaN(parsed) ? undefined : parsed;
  }
  return undefined;
}

export function buildVerboseSteps(
  spans: SpanSummary[],
  traceStartMs?: number
): VerboseStep[] {
  if (!spans.length) return [];

  const sorted = [...spans].sort(
    (a, b) => new Date(a.start_time).getTime() - new Date(b.start_time).getTime()
  );

  return sorted
    .map((span) => {
      const attrs = span.span_attributes || {};
      const eventType = attrs["event.type"] as string | undefined;
      const toolName = attrs["tool.name"] as string | undefined;
      const baseTitle = TITLE_MAP[span.name] || span.name;
      const title =
        eventType === "tool.call" && toolName
          ? `Tool Call: ${toolName}`
          : eventType === "llm.call"
            ? "LLM Call"
            : baseTitle;

      const payloadSize = getNumber(attrs["telemetry.payload_size_bytes"]);
      const payloadHash = attrs["telemetry.payload_sha256"] as string | undefined;
      const payloadTruncated = Boolean(attrs["telemetry.payload_truncated"]);

      const artifacts: VerboseArtifact[] = ARTIFACT_KEYS.flatMap((artifact) => {
        const raw = attrs[artifact.key];
        if (raw == null) return [];
        const parsed = safeParsePayload(raw);
        return [
          {
            id: `${span.span_id}-${artifact.key}`,
            title: artifact.title,
            payloadType: artifact.key,
            content: parsed,
            size: payloadSize,
            hash: payloadHash,
            isTruncated: payloadTruncated,
            isRedacted: containsRedaction(parsed)
          }
        ];
      });

      if (!eventType && artifacts.length === 0) {
        return null;
      }

      const startMs = new Date(span.start_time).getTime();
      const startOffsetMs = traceStartMs ? Math.max(startMs - traceStartMs, 0) : 0;
      const status =
        span.status_code === "ERROR" || attrs["telemetry.error_json"]
          ? "error"
          : "ok";

      return {
        id: span.span_id,
        title,
        status,
        durationMs: span.duration_ms,
        startOffsetMs,
        spanId: span.span_id,
        traceId: span.trace_id,
        artifacts
      };
    })
    .filter((step): step is VerboseStep => Boolean(step));
}
