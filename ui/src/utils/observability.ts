type TimestampInput = number | string | Date | null | undefined;

interface FormatTimestampOptions {
  inputInSeconds?: boolean;
  style?: "time" | "datetime";
  fallback?: string;
}

export interface NormalizedDecisionEvent {
  event: any;
  timestampMs: number | null;
  key: string;
}

export interface CopyBundleMessageInput {
  sql?: string;
  traceId?: string;
  validationSummary?: any;
  validationReport?: any;
  resultCompleteness?: any;
}

function readFirstNonEmptyString(values: unknown[]): string | undefined {
  for (const value of values) {
    if (typeof value === "string" && value.trim()) {
      return value.trim();
    }
  }
  return undefined;
}

function normalizeRetryCount(value: unknown): number {
  if (typeof value !== "number" || !Number.isFinite(value)) return 0;
  return Math.max(0, Math.trunc(value));
}

function toTimestampMs(
  value: TimestampInput,
  inputInSeconds: boolean = false
): number | null {
  if (value == null) return null;

  if (value instanceof Date) {
    const dateMs = value.getTime();
    return Number.isFinite(dateMs) ? dateMs : null;
  }

  if (typeof value === "number" && Number.isFinite(value)) {
    if (inputInSeconds) return value * 1000;
    return value > 1e12 ? value : value * 1000;
  }

  if (typeof value === "string" && value.trim()) {
    const asNumber = Number(value);
    if (Number.isFinite(asNumber)) {
      if (inputInSeconds) return asNumber * 1000;
      return asNumber > 1e12 ? asNumber : asNumber * 1000;
    }
    const parsed = Date.parse(value);
    if (Number.isFinite(parsed)) return parsed;
  }

  return null;
}

export function formatTimestamp(
  value: TimestampInput,
  options: FormatTimestampOptions = {}
): string {
  const fallback = options.fallback ?? "—";
  const ms = toTimestampMs(value, options.inputInSeconds);
  if (ms == null) return fallback;

  const date = new Date(ms);
  if (options.style === "time") {
    return date.toLocaleTimeString();
  }
  return date.toLocaleString();
}

export function toPrettyJson(value: unknown): string {
  if (value === undefined) return "";
  try {
    return JSON.stringify(value, null, 2);
  } catch {
    return String(value);
  }
}

export async function copyTextToClipboard(text: string): Promise<boolean> {
  if (typeof navigator === "undefined" || !navigator.clipboard?.writeText) {
    return false;
  }
  try {
    await navigator.clipboard.writeText(text);
    return true;
  } catch {
    return false;
  }
}

export function buildCopyBundlePayload(message: CopyBundleMessageInput): Record<string, unknown> {
  const summary = message.validationSummary;
  const report = message.validationReport;
  const completeness = message.resultCompleteness;
  const syntaxCount = Array.isArray(summary?.syntax_errors) ? summary.syntax_errors.length : 0;
  const missingCount = Array.isArray(summary?.missing_identifiers) ? summary.missing_identifiers.length : 0;
  const validationFailed = Boolean(summary?.ast_valid === false || syntaxCount > 0 || missingCount > 0);
  const cartesianRisk = Boolean(
    report?.detected_cartesian_flag ||
    report?.metadata?.detected_cartesian_flag ||
    summary?.detected_cartesian_flag
  );

  let completenessStatus = "complete";
  if (completeness?.token_expired) completenessStatus = "token expired";
  else if (completeness?.schema_mismatch) completenessStatus = "schema mismatch";
  else if (completeness?.is_truncated || completeness?.is_limited) completenessStatus = "truncated";
  else if (completeness?.next_page_token) completenessStatus = "paginated";

  const pagesFetched =
    typeof completeness?.pages_fetched === "number"
      ? completeness.pages_fetched
      : completeness
        ? 1
        : null;

  const payload: Record<string, unknown> = {
    sql: message.sql ?? null,
    validation: {
      status: validationFailed ? "fail" : "pass",
      cartesian_risk: cartesianRisk,
      validation_summary: summary ?? null,
      validation_report: report ?? null,
    },
    completeness: {
      status: completenessStatus,
      pages_fetched: pagesFetched,
      completeness_summary: completeness ?? null,
    },
  };
  const traceId = typeof message.traceId === "string" ? message.traceId.trim() : "";
  if (traceId) {
    payload.trace_id = traceId;
  }
  return payload;
}

function normalizeDecisionEvent(
  rawEvent: unknown,
  originalIndex: number
): { event: Record<string, unknown>; timestampMs: number | null } {
  const source: Record<string, unknown> =
    rawEvent && typeof rawEvent === "object" && !Array.isArray(rawEvent)
      ? { ...(rawEvent as Record<string, unknown>) }
      : typeof rawEvent === "string" && rawEvent.trim()
        ? { message: rawEvent.trim() }
        : { raw_event: rawEvent };

  const timestampCandidate = [
    source.timestamp,
    source.ts,
    source.time,
    source.created_at,
    source.occurred_at,
  ].find((value) => value != null) as TimestampInput;
  const timestampMs = toTimestampMs(timestampCandidate);

  const node = readFirstNonEmptyString([source.node, source.phase]) ?? "Agent";
  const phase = readFirstNonEmptyString([source.phase, source.node]);
  const decision =
    readFirstNonEmptyString([
      source.decision,
      source.message,
      source.action,
      source.event_type,
      source.type,
      source.level,
    ]) ?? "(no decision recorded)";
  const reason = readFirstNonEmptyString([
    source.reason,
    source.detail,
    source.description,
    source.message,
  ]) ?? "";
  const type = readFirstNonEmptyString([source.type, source.event_type, source.action]);

  return {
    event: {
      ...source,
      node,
      phase: phase ?? source.phase,
      decision,
      reason,
      type,
      event_type: source.event_type ?? type,
      retry_count: normalizeRetryCount(source.retry_count),
    },
    timestampMs,
  };
}

export function normalizeDecisionEvents(
  events: unknown[] | null | undefined
): NormalizedDecisionEvent[] {
  if (!Array.isArray(events) || events.length === 0) return [];

  const sorted = events
    .map((rawEvent, originalIndex) => {
      const normalized = normalizeDecisionEvent(rawEvent, originalIndex);
      return {
        event: normalized.event,
        originalIndex,
        timestampMs: normalized.timestampMs,
      };
    })
    .sort((left, right) => {
      if (left.timestampMs != null && right.timestampMs != null && left.timestampMs !== right.timestampMs) {
        return left.timestampMs - right.timestampMs;
      }
      if (left.timestampMs != null && right.timestampMs == null) return -1;
      if (left.timestampMs == null && right.timestampMs != null) return 1;
      return left.originalIndex - right.originalIndex;
    });

  const eventFingerprintCounts = new Map<string, number>();
  return sorted.map((item, index) => {
    if (typeof item.event?.id === "string" && item.event.id.trim()) {
      return { event: item.event, timestampMs: item.timestampMs, key: item.event.id };
    }
    if (typeof item.event?.event_id === "string" && item.event.event_id.trim()) {
      return { event: item.event, timestampMs: item.timestampMs, key: item.event.event_id };
    }

    const fingerprint = [
      String(item.event?.timestamp ?? ""),
      String(item.event?.node ?? ""),
      String(item.event?.decision ?? ""),
      String(item.event?.reason ?? ""),
      String(item.event?.error_category ?? ""),
      String(item.event?.retry_count ?? ""),
    ].join("|");
    const seen = eventFingerprintCounts.get(fingerprint) ?? 0;
    eventFingerprintCounts.set(fingerprint, seen + 1);
    return {
      event: item.event,
      timestampMs: item.timestampMs,
      key: `${fingerprint}|${seen}|${index}`,
    };
  });
}
